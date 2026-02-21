//! HTTP サーバー（Kotlin版 HttpGateway 互換）
//!
//! 位置づけ:
//! - このモジュールは「HTTP入口層」。注文受付→FastPath投入→監査/Bus/SSEへの橋渡しを担う。
//! - 入口のルーティングをここに集約し、実処理はサブモジュールに分離する。
//!
//! ハンドラの分類（同期境界 / ユーザ用 / 運用用）:
//! - 同期境界（ACKまで）:
//!   - /orders: 低遅延で受理する入口。ここまでが同期境界。
//! - ユーザ向け（注文/通知）:
//!   - /orders/{id}: 受理後の現在状態を取得（UI確認）。
//!   - /orders/{id}/cancel: キャンセル要求を非同期で流す。
//!   - /orders/{id}/stream: 注文単位のリアルタイム通知。
//!   - /stream: アカウント全体のリアルタイム通知。
//! - 運用/監査向け:
//!   - /orders/{id}/events: 監査ログ由来の履歴取得（調査）。
//!   - /accounts/{id}/events: アカウント全体の監査履歴。
//!   - /audit/verify: 監査ログの整合性検証（改ざん検知）。
//!   - /audit/anchor: 監査ハッシュのスナップショット取得。
//!   - /health: 稼働確認・簡易SLO確認。
//!   - /metrics: Prometheus用の観測出力。

// ハンドラはドメイン別に分割（注文 / 監査 / SSE / メトリクス）
mod audit;
mod metrics;
mod orders;
mod sse;

use axum::{
    routing::{get, post},
    Router,
};
use gateway_core::SymbolLimits;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, error::TrySendError};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::audit::AuditDurableNotification;
use crate::audit::AuditLog;
use crate::auth::JwtAuth;
use crate::backpressure::BackpressureConfig;
use crate::bus::BusPublisher;
use crate::engine::FastPathEngine;
use crate::inflight::InflightControllerHandle;
use crate::rate_limit::AccountRateLimiter;
use crate::sse::SseHub;
use crate::store::{OrderIdMap, OrderStore, ShardedOrderStore};
use gateway_core::LatencyHistogram;
use std::path::PathBuf;

use audit::{handle_account_events, handle_audit_anchor, handle_audit_verify, handle_order_events};
use metrics::{handle_health, handle_metrics};
use orders::{
    handle_cancel_order, handle_get_order, handle_get_order_by_client_id, handle_get_order_v2,
    handle_order, handle_order_v2, handle_order_v3,
};
use sse::{handle_account_stream, handle_order_stream};

/// /v3/orders の single-writer に流す最小タスク。
#[derive(Debug)]
pub(super) struct V3OrderTask {
    pub(super) _session_seq: u64,
    pub(super) _received_at_ns: u64,
}

#[derive(Clone, Copy)]
pub(super) enum V3RiskProfile {
    Light,
    Medium,
    Heavy,
}

impl V3RiskProfile {
    pub(super) fn from_env() -> Self {
        match std::env::var("V3_RISK_PROFILE")
            .unwrap_or_else(|_| "light".to_string())
            .to_lowercase()
            .as_str()
        {
            "medium" => Self::Medium,
            "heavy" => Self::Heavy,
            _ => Self::Light,
        }
    }

    pub(super) fn as_metric_level(self) -> u64 {
        match self {
            Self::Light => 1,
            Self::Medium => 2,
            Self::Heavy => 3,
        }
    }

    pub(super) fn loops(self) -> u32 {
        match self {
            Self::Light => 16,
            Self::Medium => 256,
            Self::Heavy => 2_048,
        }
    }
}

#[derive(Clone, Copy)]
pub(super) enum V3RiskMarginMode {
    Legacy,
    Incremental,
}

impl V3RiskMarginMode {
    pub(super) fn from_env() -> Self {
        match std::env::var("V3_RISK_MARGIN_MODE")
            .unwrap_or_else(|_| "legacy".to_string())
            .to_lowercase()
            .as_str()
        {
            "incremental" => Self::Incremental,
            _ => Self::Legacy,
        }
    }

    pub(super) fn as_metric_level(self) -> u64 {
        match self {
            Self::Legacy => 1,
            Self::Incremental => 2,
        }
    }
}

/// /v3/orders の入口制御と single-writer キューを管理するハンドル。
#[derive(Clone)]
pub(super) struct V3Ingress {
    tx: Sender<V3OrderTask>,
    depth: Arc<AtomicU64>,
    max_depth: u64,
    seq: Arc<AtomicU64>,
    kill_switch: Arc<AtomicBool>,
    kill_since_ns: Arc<AtomicU64>,
    kill_auto_recover: bool,
    kill_recover_pct: u64,
    kill_recover_after_ns: u64,
    processed_total: Arc<AtomicU64>,
}

impl V3Ingress {
    fn new(
        tx: Sender<V3OrderTask>,
        max_depth: u64,
        kill_auto_recover: bool,
        kill_recover_pct: u64,
        kill_recover_after_ms: u64,
    ) -> Self {
        Self {
            tx,
            depth: Arc::new(AtomicU64::new(0)),
            max_depth,
            seq: Arc::new(AtomicU64::new(1)),
            kill_switch: Arc::new(AtomicBool::new(false)),
            kill_since_ns: Arc::new(AtomicU64::new(0)),
            kill_auto_recover,
            kill_recover_pct,
            kill_recover_after_ns: kill_recover_after_ms.saturating_mul(1_000_000),
            processed_total: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(super) fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    pub(super) fn depth(&self) -> u64 {
        self.depth.load(Ordering::Relaxed)
    }

    pub(super) fn max_depth(&self) -> u64 {
        self.max_depth
    }

    pub(super) fn queue_utilization_pct(&self) -> u64 {
        if self.max_depth == 0 {
            100
        } else {
            self.depth().saturating_mul(100) / self.max_depth
        }
    }

    pub(super) fn is_killed(&self) -> bool {
        self.kill_switch.load(Ordering::Relaxed)
    }

    pub(super) fn kill(&self, now_ns: u64) {
        if !self.kill_switch.swap(true, Ordering::Relaxed) {
            self.kill_since_ns.store(now_ns, Ordering::Relaxed);
        }
    }

    pub(super) fn maybe_recover(&self, now_ns: u64) -> bool {
        if !self.kill_auto_recover || !self.is_killed() {
            return false;
        }
        if self.queue_utilization_pct() > self.kill_recover_pct {
            return false;
        }
        let since = self.kill_since_ns.load(Ordering::Relaxed);
        if since == 0 {
            return false;
        }
        if now_ns.saturating_sub(since) < self.kill_recover_after_ns {
            return false;
        }
        if self.kill_switch.swap(false, Ordering::Relaxed) {
            self.kill_since_ns.store(0, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub(super) fn kill_auto_recover_enabled(&self) -> bool {
        self.kill_auto_recover
    }

    pub(super) fn kill_recover_pct(&self) -> u64 {
        self.kill_recover_pct
    }

    pub(super) fn kill_recover_after_ms(&self) -> u64 {
        self.kill_recover_after_ns / 1_000_000
    }

    pub(super) fn processed_total(&self) -> u64 {
        self.processed_total.load(Ordering::Relaxed)
    }

    fn on_processed_one(&self) {
        let prev = self.depth.fetch_sub(1, Ordering::Relaxed);
        if prev == 0 {
            self.depth.store(0, Ordering::Relaxed);
        }
        self.processed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn try_enqueue(&self, task: V3OrderTask) -> Result<(), TrySendError<V3OrderTask>> {
        match self.tx.try_send(task) {
            Ok(()) => {
                self.depth.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }
}

/// アプリケーション状態
#[derive(Clone)]
pub(super) struct AppState {
    pub(super) engine: FastPathEngine,
    pub(super) jwt_auth: Arc<JwtAuth>,
    /// レガシー OrderStore（後方互換用）
    #[allow(dead_code)]
    pub(super) order_store: Arc<OrderStore>,
    /// HFT最適化版 ShardedOrderStore
    pub(super) sharded_store: Arc<ShardedOrderStore>,
    /// 注文IDマッピング（内部ID ↔ 外部ID）
    pub(super) order_id_map: Arc<OrderIdMap>,
    pub(super) sse_hub: Arc<SseHub>,
    pub(super) order_id_seq: Arc<AtomicU64>,
    pub(super) audit_log: Arc<AuditLog>,
    pub(super) audit_read_path: Arc<PathBuf>,
    pub(super) bus_publisher: Arc<BusPublisher>,
    pub(super) bus_mode_outbox: bool,
    pub(super) backpressure: BackpressureConfig,
    pub(super) inflight_controller: InflightControllerHandle,
    pub(super) rate_limiter: Option<Arc<AccountRateLimiter>>,
    pub(super) ack_hist: Arc<LatencyHistogram>,
    pub(super) wal_enqueue_hist: Arc<LatencyHistogram>,
    pub(super) durable_ack_hist: Arc<LatencyHistogram>,
    pub(super) fdatasync_hist: Arc<LatencyHistogram>,
    pub(super) durable_notify_hist: Arc<LatencyHistogram>,
    pub(super) idempotency_checked: Arc<AtomicU64>,
    pub(super) idempotency_hits: Arc<AtomicU64>,
    pub(super) idempotency_creates: Arc<AtomicU64>,
    pub(super) reject_invalid_qty: Arc<AtomicU64>,
    pub(super) reject_rate_limit: Arc<AtomicU64>,
    pub(super) reject_risk: Arc<AtomicU64>,
    pub(super) reject_invalid_symbol: Arc<AtomicU64>,
    pub(super) reject_queue_full: Arc<AtomicU64>,
    pub(super) backpressure_soft_wal_age: Arc<AtomicU64>,
    pub(super) backpressure_soft_rate_decline: Arc<AtomicU64>,
    pub(super) backpressure_inflight: Arc<AtomicU64>,
    pub(super) backpressure_wal_bytes: Arc<AtomicU64>,
    pub(super) backpressure_wal_age: Arc<AtomicU64>,
    pub(super) backpressure_disk_free: Arc<AtomicU64>,
    pub(super) v3_ingress: Arc<V3Ingress>,
    pub(super) v3_accepted_total: Arc<AtomicU64>,
    pub(super) v3_rejected_soft_total: Arc<AtomicU64>,
    pub(super) v3_rejected_killed_total: Arc<AtomicU64>,
    pub(super) v3_kill_recovered_total: Arc<AtomicU64>,
    pub(super) v3_soft_reject_pct: u64,
    pub(super) v3_kill_reject_pct: u64,
    pub(super) v3_risk_profile: V3RiskProfile,
    pub(super) v3_risk_margin_mode: V3RiskMarginMode,
    pub(super) v3_risk_loops: u32,
    pub(super) v3_risk_strict_symbols: bool,
    pub(super) v3_risk_max_order_qty: u64,
    pub(super) v3_risk_max_notional: u64,
    pub(super) v3_risk_daily_notional_limit: u64,
    pub(super) v3_risk_max_abs_position_qty: u64,
    pub(super) v3_symbol_limits: Arc<HashMap<[u8; 8], SymbolLimits>>,
    pub(super) v3_account_daily_notional:
        Arc<dashmap::DashMap<String, Arc<gateway_core::AccountPosition>>>,
    pub(super) v3_account_symbol_position: Arc<dashmap::DashMap<(String, [u8; 8]), i64>>,
    pub(super) v3_stage_parse_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_position_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_margin_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_limits_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_enqueue_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_serialize_hist: Arc<LatencyHistogram>,
}

/// 認証エラーレスポンス
#[derive(serde::Serialize)]
pub(super) struct AuthErrorResponse {
    pub(super) error: String,
}

fn parse_bool_env(key: &str) -> Option<bool> {
    std::env::var(key)
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Some(true),
            "0" | "false" | "no" | "off" => Some(false),
            _ => None,
        })
}

fn parse_v3_symbol_limits(default_max_order_qty: u64, default_max_notional: u64) -> HashMap<[u8; 8], SymbolLimits> {
    let raw = std::env::var("V3_RISK_SYMBOL_LIMITS").ok();
    let mut limits = HashMap::new();

    let fallback_symbols = ["AAPL", "MSFT", "NVDA", "BTC", "ETH", "SOL"];
    let default_limit = SymbolLimits {
        max_order_qty: default_max_order_qty.min(u32::MAX as u64) as u32,
        max_notional: default_max_notional,
        tick_size: 1,
    };

    match raw {
        Some(spec) if !spec.trim().is_empty() => {
            for item in spec.split(',') {
                let trimmed = item.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let mut parts = trimmed.split(':');
                let symbol = parts.next().unwrap_or("").trim().to_uppercase();
                if symbol.is_empty() || symbol.len() > 8 {
                    continue;
                }
                let max_qty = parts
                    .next()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(default_max_order_qty)
                    .min(u32::MAX as u64) as u32;
                let max_notional = parts
                    .next()
                    .and_then(|v| v.trim().parse::<u64>().ok())
                    .filter(|v| *v > 0)
                    .unwrap_or(default_max_notional);

                limits.insert(
                    FastPathEngine::symbol_to_bytes(&symbol),
                    SymbolLimits {
                        max_order_qty: max_qty,
                        max_notional,
                        tick_size: 1,
                    },
                );
            }
        }
        _ => {
            for symbol in fallback_symbols {
                limits.insert(FastPathEngine::symbol_to_bytes(symbol), default_limit);
            }
        }
    }

    limits
}

/// HTTPサーバーを起動
pub async fn run(
    port: u16,
    engine: FastPathEngine,
    order_store: Arc<OrderStore>,
    sse_hub: Arc<SseHub>,
    audit_log: Arc<AuditLog>,
    bus_publisher: Arc<BusPublisher>,
    bus_mode_outbox: bool,
    idempotency_ttl_sec: u64,
    durable_rx: Option<UnboundedReceiver<AuditDurableNotification>>,
) -> anyhow::Result<()> {
    let shard_count = std::env::var("ORDER_STORE_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(64);
    info!("ShardedOrderStore configured (shards={})", shard_count);

    let inflight_controller = crate::inflight::InflightController::spawn_from_env();
    let mut backpressure = BackpressureConfig::from_env();
    if inflight_controller.enabled() {
        backpressure.inflight_max = None;
    }
    let rate_limiter = crate::rate_limit::AccountRateLimiter::from_env().map(Arc::new);
    let v3_risk_profile = V3RiskProfile::from_env();
    let v3_risk_margin_mode = V3RiskMarginMode::from_env();
    let v3_risk_loops = std::env::var("V3_RISK_LOOPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(v3_risk_profile.loops());
    let v3_risk_strict_symbols = parse_bool_env("V3_RISK_STRICT_SYMBOLS").unwrap_or(true);
    let v3_risk_max_order_qty = std::env::var("MAX_ORDER_QTY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(10_000);
    let v3_risk_max_notional = std::env::var("MAX_NOTIONAL")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1_000_000_000);
    let v3_risk_daily_notional_limit = std::env::var("V3_RISK_DAILY_NOTIONAL_LIMIT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(v3_risk_max_notional.saturating_mul(1_000));
    let v3_risk_max_abs_position_qty = std::env::var("V3_RISK_MAX_ABS_POSITION_QTY")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(v3_risk_max_order_qty.saturating_mul(10_000));
    let v3_symbol_limits = Arc::new(parse_v3_symbol_limits(
        v3_risk_max_order_qty,
        v3_risk_max_notional,
    ));
    let v3_kill_auto_recover = parse_bool_env("V3_KILL_AUTO_RECOVER").unwrap_or(true);
    let v3_soft_reject_pct = std::env::var("V3_SOFT_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(99))
        .unwrap_or(85);
    let mut v3_kill_reject_pct = std::env::var("V3_KILL_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(100))
        .unwrap_or(95);
    if v3_kill_reject_pct <= v3_soft_reject_pct {
        v3_kill_reject_pct = (v3_soft_reject_pct + 1).min(100);
    }
    let v3_kill_recover_pct = std::env::var("V3_KILL_RECOVER_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(94))
        .unwrap_or(60);
    let v3_kill_recover_after_ms = std::env::var("V3_KILL_RECOVER_AFTER_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(3_000);
    let v3_queue_capacity = std::env::var("V3_INGRESS_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(65_536);
    let (v3_tx, v3_rx) = tokio::sync::mpsc::channel(v3_queue_capacity);
    let v3_ingress = Arc::new(V3Ingress::new(
        v3_tx,
        v3_queue_capacity as u64,
        v3_kill_auto_recover,
        v3_kill_recover_pct,
        v3_kill_recover_after_ms,
    ));

    let audit_read_path = {
        let configured = std::env::var("AUDIT_LOG_PATH").ok().map(PathBuf::from);
        if let Some(path) = configured {
            path
        } else if std::env::var("AUDIT_MIRROR_ENABLE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        {
            PathBuf::from(
                std::env::var("AUDIT_MIRROR_PATH")
                    .unwrap_or_else(|_| "var/gateway/audit.mirror.log".into()),
            )
        } else {
            audit_log.path().to_path_buf()
        }
    };

    let state = AppState {
        engine,
        jwt_auth: Arc::new(JwtAuth::from_env()),
        order_store,
        sharded_store: Arc::new(ShardedOrderStore::new_with_ttl_and_shards(
            idempotency_ttl_sec * 1000,
            shard_count,
        )),
        order_id_map: Arc::new(OrderIdMap::new()),
        sse_hub,
        order_id_seq: Arc::new(AtomicU64::new(1)),
        audit_log,
        audit_read_path: Arc::new(audit_read_path),
        bus_publisher,
        bus_mode_outbox,
        backpressure,
        inflight_controller,
        rate_limiter,
        ack_hist: Arc::new(LatencyHistogram::new()),
        wal_enqueue_hist: Arc::new(LatencyHistogram::new()),
        durable_ack_hist: Arc::new(LatencyHistogram::new()),
        fdatasync_hist: Arc::new(LatencyHistogram::new()),
        durable_notify_hist: Arc::new(LatencyHistogram::new()),
        idempotency_checked: Arc::new(AtomicU64::new(0)),
        idempotency_hits: Arc::new(AtomicU64::new(0)),
        idempotency_creates: Arc::new(AtomicU64::new(0)),
        reject_invalid_qty: Arc::new(AtomicU64::new(0)),
        reject_rate_limit: Arc::new(AtomicU64::new(0)),
        reject_risk: Arc::new(AtomicU64::new(0)),
        reject_invalid_symbol: Arc::new(AtomicU64::new(0)),
        reject_queue_full: Arc::new(AtomicU64::new(0)),
        backpressure_soft_wal_age: Arc::new(AtomicU64::new(0)),
        backpressure_soft_rate_decline: Arc::new(AtomicU64::new(0)),
        backpressure_inflight: Arc::new(AtomicU64::new(0)),
        backpressure_wal_bytes: Arc::new(AtomicU64::new(0)),
        backpressure_wal_age: Arc::new(AtomicU64::new(0)),
        backpressure_disk_free: Arc::new(AtomicU64::new(0)),
        v3_ingress: Arc::clone(&v3_ingress),
        v3_accepted_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_soft_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_killed_total: Arc::new(AtomicU64::new(0)),
        v3_kill_recovered_total: Arc::new(AtomicU64::new(0)),
        v3_soft_reject_pct,
        v3_kill_reject_pct,
        v3_risk_profile,
        v3_risk_margin_mode,
        v3_risk_loops,
        v3_risk_strict_symbols,
        v3_risk_max_order_qty,
        v3_risk_max_notional,
        v3_risk_daily_notional_limit,
        v3_risk_max_abs_position_qty,
        v3_symbol_limits,
        v3_account_daily_notional: Arc::new(dashmap::DashMap::new()),
        v3_account_symbol_position: Arc::new(dashmap::DashMap::new()),
        v3_stage_parse_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_position_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_margin_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_limits_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_enqueue_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_serialize_hist: Arc::new(LatencyHistogram::new()),
    };

    tokio::spawn(async move {
        run_v3_single_writer(v3_rx, v3_ingress).await;
    });

    if let Some(rx) = durable_rx {
        let durable_state = state.clone();
        tokio::spawn(async move {
            run_durable_notifier(rx, durable_state).await;
        });
    }

    let app = Router::new()
        .route("/orders", post(handle_order))
        .route("/v2/orders", post(handle_order_v2))
        .route("/v3/orders", post(handle_order_v3))
        .route("/orders/{order_id}", get(handle_get_order))
        .route("/v2/orders/{order_id}", get(handle_get_order_v2))
        .route(
            "/orders/client/{client_order_id}",
            get(handle_get_order_by_client_id),
        )
        .route(
            "/v2/orders/client/{client_order_id}",
            get(handle_get_order_by_client_id),
        )
        .route("/orders/{order_id}/cancel", post(handle_cancel_order))
        .route("/v2/orders/{order_id}/cancel", post(handle_cancel_order))
        .route("/orders/{order_id}/events", get(handle_order_events))
        .route("/accounts/{account_id}/events", get(handle_account_events))
        .route("/orders/{order_id}/stream", get(handle_order_stream))
        .route("/stream", get(handle_account_stream))
        .route("/audit/verify", get(handle_audit_verify))
        .route("/audit/anchor", get(handle_audit_anchor))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("HTTP server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

/// /v3/orders の single-writer worker。
/// Phase1ではキュー整合と入口計測を成立させるため、直列消費のみを行う。
async fn run_v3_single_writer(mut rx: Receiver<V3OrderTask>, ingress: Arc<V3Ingress>) {
    while let Some(_task) = rx.recv().await {
        ingress.on_processed_one();
    }
}

async fn run_durable_notifier(
    mut rx: UnboundedReceiver<AuditDurableNotification>,
    state: AppState,
) {
    while let Some(note) = rx.recv().await {
        let notify_start_ns = gateway_core::now_nanos();
        if note.durable_done_ns >= note.request_start_ns && note.request_start_ns > 0 {
            let elapsed_us = (note.durable_done_ns - note.request_start_ns) / 1_000;
            state.durable_ack_hist.record(elapsed_us);
        }
        if note.fdatasync_ns > 0 {
            state.fdatasync_hist.record(note.fdatasync_ns / 1_000);
        }

        let durable_latency_us = if note.durable_done_ns >= note.request_start_ns {
            (note.durable_done_ns - note.request_start_ns) / 1_000
        } else {
            0
        };
        let data = serde_json::json!({
            "eventType": note.event_type,
            "durableLatencyUs": durable_latency_us,
        })
        .to_string();

        if note.event_type == "OrderAccepted" {
            if let Some(order_id) = note.order_id.as_deref() {
                state
                    .sharded_store
                    .mark_durable(order_id, &note.account_id, note.event_at);
                state.inflight_controller.on_commit(1);
                state
                    .sse_hub
                    .publish_order(order_id, "order_durable", &data);
            }
            state
                .sse_hub
                .publish_account(&note.account_id, "order_durable", &data);
        }

        // durable完了後の通知経路（channel受信〜SSE/状態更新）を観測。
        if note.durable_done_ns > 0 {
            let notify_us = gateway_core::now_nanos().saturating_sub(note.durable_done_ns) / 1_000;
            state.durable_notify_hist.record(notify_us);
        } else {
            let notify_us = gateway_core::now_nanos().saturating_sub(notify_start_ns) / 1_000;
            state.durable_notify_hist.record(notify_us);
        }
    }
}
