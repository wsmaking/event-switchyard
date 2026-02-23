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
    Router,
    routing::{get, post},
};
use gateway_core::SymbolLimits;
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc::{Receiver, Sender, UnboundedReceiver, error::TrySendError};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::audit::AuditLog;
use crate::audit::{AuditDurableNotification, AuditEvent};
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
    handle_get_order_v3, handle_order, handle_order_v2, handle_order_v3,
};
use sse::{handle_account_stream, handle_order_stream};

/// /v3/orders の single-writer に流す最小タスク。
#[derive(Debug)]
pub(super) struct V3OrderTask {
    pub(super) session_id: String,
    pub(super) session_seq: u64,
    pub(super) attempt_seq: u64,
    pub(super) received_at_ns: u64,
    pub(super) shard_id: usize,
}

#[derive(Debug, Clone)]
pub(super) struct V3DurableTask {
    pub(super) session_id: String,
    pub(super) session_seq: u64,
    pub(super) attempt_seq: u64,
    pub(super) received_at_ns: u64,
    pub(super) shard_id: usize,
}

impl From<V3OrderTask> for V3DurableTask {
    fn from(task: V3OrderTask) -> Self {
        Self {
            session_id: task.session_id,
            session_seq: task.session_seq,
            attempt_seq: task.attempt_seq,
            received_at_ns: task.received_at_ns,
            shard_id: task.shard_id,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum V3ConfirmStatus {
    VolatileAccept,
    DurableAccepted,
    DurableRejected,
    LossSuspect,
}

impl V3ConfirmStatus {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::VolatileAccept => "VOLATILE_ACCEPT",
            Self::DurableAccepted => "DURABLE_ACCEPTED",
            Self::DurableRejected => "DURABLE_REJECTED",
            Self::LossSuspect => "LOSS_SUSPECT",
        }
    }
}

#[derive(Clone, Debug)]
struct V3ConfirmRecord {
    status: V3ConfirmStatus,
    reason: Option<String>,
    attempt_seq: u64,
    received_at_ns: u64,
    updated_at_ns: u64,
    shard_id: usize,
}

#[derive(Clone, Debug)]
pub(super) struct V3ConfirmSnapshot {
    pub(super) status: V3ConfirmStatus,
    pub(super) reason: Option<String>,
    pub(super) attempt_seq: u64,
    pub(super) received_at_ns: u64,
    pub(super) updated_at_ns: u64,
    pub(super) shard_id: usize,
}

#[derive(Clone, Debug)]
pub(super) struct V3LossCandidate {
    pub(super) session_id: String,
    pub(super) session_seq: u64,
    pub(super) shard_id: usize,
}

#[derive(Clone, Default)]
pub(super) struct V3ConfirmStore {
    records: Arc<dashmap::DashMap<(String, u64), V3ConfirmRecord>>,
}

impl V3ConfirmStore {
    pub(super) fn new() -> Self {
        Self {
            records: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub(super) fn record_volatile(&self, task: &V3OrderTask, now_ns: u64) {
        self.records.insert(
            (task.session_id.clone(), task.session_seq),
            V3ConfirmRecord {
                status: V3ConfirmStatus::VolatileAccept,
                reason: None,
                attempt_seq: task.attempt_seq,
                received_at_ns: task.received_at_ns,
                updated_at_ns: now_ns,
                shard_id: task.shard_id,
            },
        );
    }

    pub(super) fn mark_durable_accepted(&self, session_id: &str, session_seq: u64, now_ns: u64) {
        if let Some(mut entry) = self.records.get_mut(&(session_id.to_string(), session_seq)) {
            entry.status = V3ConfirmStatus::DurableAccepted;
            entry.reason = None;
            entry.updated_at_ns = now_ns;
            return;
        }
        self.records.insert(
            (session_id.to_string(), session_seq),
            V3ConfirmRecord {
                status: V3ConfirmStatus::DurableAccepted,
                reason: None,
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id: 0,
            },
        );
    }

    pub(super) fn mark_durable_rejected(
        &self,
        session_id: &str,
        session_seq: u64,
        reason: &str,
        now_ns: u64,
    ) {
        if let Some(mut entry) = self.records.get_mut(&(session_id.to_string(), session_seq)) {
            entry.status = V3ConfirmStatus::DurableRejected;
            entry.reason = Some(reason.to_string());
            entry.updated_at_ns = now_ns;
            return;
        }
        self.records.insert(
            (session_id.to_string(), session_seq),
            V3ConfirmRecord {
                status: V3ConfirmStatus::DurableRejected,
                reason: Some(reason.to_string()),
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id: 0,
            },
        );
    }

    pub(super) fn mark_loss_suspect(
        &self,
        session_id: &str,
        session_seq: u64,
        shard_id: usize,
        reason: &str,
        now_ns: u64,
    ) -> Option<usize> {
        if let Some(mut entry) = self.records.get_mut(&(session_id.to_string(), session_seq)) {
            if entry.status == V3ConfirmStatus::LossSuspect {
                return None;
            }
            if entry.status == V3ConfirmStatus::DurableAccepted
                || entry.status == V3ConfirmStatus::DurableRejected
            {
                return None;
            }
            entry.status = V3ConfirmStatus::LossSuspect;
            entry.reason = Some(reason.to_string());
            entry.updated_at_ns = now_ns;
            return Some(entry.shard_id);
        }
        self.records.insert(
            (session_id.to_string(), session_seq),
            V3ConfirmRecord {
                status: V3ConfirmStatus::LossSuspect,
                reason: Some(reason.to_string()),
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id,
            },
        );
        Some(shard_id)
    }

    pub(super) fn snapshot(&self, session_id: &str, session_seq: u64) -> Option<V3ConfirmSnapshot> {
        self.records
            .get(&(session_id.to_string(), session_seq))
            .map(|entry| V3ConfirmSnapshot {
                status: entry.status,
                reason: entry.reason.clone(),
                attempt_seq: entry.attempt_seq,
                received_at_ns: entry.received_at_ns,
                updated_at_ns: entry.updated_at_ns,
                shard_id: entry.shard_id,
            })
    }

    pub(super) fn collect_timed_out(
        &self,
        now_ns: u64,
        timeout_ns: u64,
        max_scan: usize,
    ) -> Vec<V3LossCandidate> {
        let mut out = Vec::new();
        for item in self.records.iter() {
            if out.len() >= max_scan {
                break;
            }
            if item.status != V3ConfirmStatus::VolatileAccept {
                continue;
            }
            if now_ns.saturating_sub(item.received_at_ns) < timeout_ns {
                continue;
            }
            out.push(V3LossCandidate {
                session_id: item.key().0.clone(),
                session_seq: item.key().1,
                shard_id: item.shard_id,
            });
        }
        out
    }
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
struct V3ShardIngress {
    tx: Sender<V3OrderTask>,
    depth: Arc<AtomicU64>,
    max_depth: u64,
    kill_switch: Arc<AtomicBool>,
    kill_since_ns: Arc<AtomicU64>,
    processed_total: Arc<AtomicU64>,
}

impl V3ShardIngress {
    fn new(tx: Sender<V3OrderTask>, max_depth: u64) -> Self {
        Self {
            tx,
            depth: Arc::new(AtomicU64::new(0)),
            max_depth,
            kill_switch: Arc::new(AtomicBool::new(false)),
            kill_since_ns: Arc::new(AtomicU64::new(0)),
            processed_total: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct V3LossEscalation {
    pub(super) session_killed: bool,
    pub(super) shard_killed: bool,
    pub(super) global_killed: bool,
}

/// /v3/orders の入口制御と single-writer キューを管理するハンドル。
#[derive(Clone)]
pub(super) struct V3Ingress {
    shards: Arc<Vec<V3ShardIngress>>,
    session_seq: Arc<dashmap::DashMap<String, Arc<AtomicU64>>>,
    session_killed: Arc<dashmap::DashSet<String>>,
    global_kill_switch: Arc<AtomicBool>,
    kill_auto_recover: bool,
    kill_recover_pct: u64,
    kill_recover_after_ns: u64,
    loss_window_ns: u64,
    session_loss_suspect_threshold: usize,
    shard_loss_suspect_threshold: usize,
    global_loss_suspect_threshold: usize,
    session_loss_windows: Arc<dashmap::DashMap<String, Arc<std::sync::Mutex<VecDeque<u64>>>>>,
    shard_loss_windows: Arc<Vec<Arc<std::sync::Mutex<VecDeque<u64>>>>>,
    global_loss_window: Arc<std::sync::Mutex<VecDeque<u64>>>,
}

impl V3Ingress {
    fn new(
        shards: Vec<V3ShardIngress>,
        kill_auto_recover: bool,
        kill_recover_pct: u64,
        kill_recover_after_ms: u64,
        loss_window_sec: u64,
        session_loss_suspect_threshold: usize,
        shard_loss_suspect_threshold: usize,
        global_loss_suspect_threshold: usize,
    ) -> Self {
        let shard_len = shards.len().max(1);
        let mut windows = Vec::with_capacity(shard_len);
        for _ in 0..shard_len {
            windows.push(Arc::new(std::sync::Mutex::new(VecDeque::new())));
        }
        Self {
            shards: Arc::new(shards),
            session_seq: Arc::new(dashmap::DashMap::new()),
            session_killed: Arc::new(dashmap::DashSet::new()),
            global_kill_switch: Arc::new(AtomicBool::new(false)),
            kill_auto_recover,
            kill_recover_pct,
            kill_recover_after_ns: kill_recover_after_ms.saturating_mul(1_000_000),
            loss_window_ns: loss_window_sec.saturating_mul(1_000_000_000),
            session_loss_suspect_threshold: session_loss_suspect_threshold.max(1),
            shard_loss_suspect_threshold: shard_loss_suspect_threshold.max(1),
            global_loss_suspect_threshold: global_loss_suspect_threshold.max(1),
            session_loss_windows: Arc::new(dashmap::DashMap::new()),
            shard_loss_windows: Arc::new(windows),
            global_loss_window: Arc::new(std::sync::Mutex::new(VecDeque::new())),
        }
    }

    fn shard_index_for_session(&self, session_id: &str) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        session_id.hash(&mut hasher);
        (hasher.finish() as usize) % self.shards.len().max(1)
    }

    pub(super) fn next_seq(&self, session_id: &str) -> u64 {
        if let Some(counter) = self.session_seq.get(session_id) {
            return counter.fetch_add(1, Ordering::Relaxed);
        }
        let counter = Arc::new(AtomicU64::new(1));
        match self
            .session_seq
            .entry(session_id.to_string())
            .or_insert_with(|| Arc::clone(&counter))
            .value()
            .clone()
        {
            existing => existing.fetch_add(1, Ordering::Relaxed),
        }
    }

    pub(super) fn shard_count(&self) -> usize {
        self.shards.len()
    }

    pub(super) fn shard_for_session(&self, session_id: &str) -> usize {
        self.shard_index_for_session(session_id)
    }

    pub(super) fn total_depth(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.depth.load(Ordering::Relaxed))
            .sum::<u64>()
    }

    pub(super) fn max_depth_per_shard(&self) -> u64 {
        self.shards.first().map(|s| s.max_depth).unwrap_or(0)
    }

    pub(super) fn queue_utilization_pct(&self, shard_id: usize) -> u64 {
        let Some(shard) = self.shards.get(shard_id) else {
            return 100;
        };
        if shard.max_depth == 0 {
            100
        } else {
            shard.depth.load(Ordering::Relaxed).saturating_mul(100) / shard.max_depth
        }
    }

    pub(super) fn queue_utilization_pct_max(&self) -> u64 {
        (0..self.shards.len())
            .map(|idx| self.queue_utilization_pct(idx))
            .max()
            .unwrap_or(0)
    }

    pub(super) fn is_global_killed(&self) -> bool {
        self.global_kill_switch.load(Ordering::Relaxed)
    }

    pub(super) fn is_session_killed(&self, session_id: &str) -> bool {
        self.session_killed.contains(session_id)
    }

    pub(super) fn is_shard_killed(&self, shard_id: usize) -> bool {
        self.shards
            .get(shard_id)
            .map(|s| s.kill_switch.load(Ordering::Relaxed))
            .unwrap_or(true)
    }

    fn kill_shard(&self, shard_id: usize, now_ns: u64) -> bool {
        let Some(shard) = self.shards.get(shard_id) else {
            return false;
        };
        if !shard.kill_switch.swap(true, Ordering::Relaxed) {
            shard.kill_since_ns.store(now_ns, Ordering::Relaxed);
            return true;
        }
        false
    }

    pub(super) fn maybe_recover_shard(&self, shard_id: usize, now_ns: u64) -> bool {
        let Some(shard) = self.shards.get(shard_id) else {
            return false;
        };
        if !self.kill_auto_recover || !self.is_shard_killed(shard_id) {
            return false;
        }
        if self.queue_utilization_pct(shard_id) > self.kill_recover_pct {
            return false;
        }
        let since = shard.kill_since_ns.load(Ordering::Relaxed);
        if since == 0 {
            return false;
        }
        if now_ns.saturating_sub(since) < self.kill_recover_after_ns {
            return false;
        }
        if shard.kill_switch.swap(false, Ordering::Relaxed) {
            shard.kill_since_ns.store(0, Ordering::Relaxed);
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

    pub(super) fn session_loss_suspect_threshold(&self) -> usize {
        self.session_loss_suspect_threshold
    }

    pub(super) fn processed_total(&self) -> u64 {
        self.shards
            .iter()
            .map(|s| s.processed_total.load(Ordering::Relaxed))
            .sum::<u64>()
    }

    pub(super) fn shard_kill_switch_count(&self) -> u64 {
        self.shards
            .iter()
            .filter(|s| s.kill_switch.load(Ordering::Relaxed))
            .count() as u64
    }

    fn on_processed_one(&self, shard_id: usize) {
        let Some(shard) = self.shards.get(shard_id) else {
            return;
        };
        let prev = shard.depth.fetch_sub(1, Ordering::Relaxed);
        if prev == 0 {
            shard.depth.store(0, Ordering::Relaxed);
        }
        shard.processed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn try_enqueue(
        &self,
        shard_id: usize,
        task: V3OrderTask,
    ) -> Result<(), TrySendError<V3OrderTask>> {
        let Some(shard) = self.shards.get(shard_id) else {
            return Err(TrySendError::Closed(task));
        };
        match shard.tx.try_send(task) {
            Ok(()) => {
                shard.depth.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub(super) fn mark_loss_suspect(
        &self,
        session_id: &str,
        shard_id: usize,
        now_ns: u64,
    ) -> V3LossEscalation {
        let shard_idx = shard_id.min(self.shards.len().saturating_sub(1));
        let mut out = V3LossEscalation::default();
        if self.record_session_loss_and_count(session_id, now_ns) >= self.session_loss_suspect_threshold
        {
            if self.session_killed.insert(session_id.to_string()) {
                out.session_killed = true;
            }
        }

        if self.record_loss_and_count(&self.shard_loss_windows[shard_idx], now_ns)
            >= self.shard_loss_suspect_threshold
        {
            if self.kill_shard(shard_idx, now_ns) {
                out.shard_killed = true;
            }
        }

        if self.record_loss_and_count(&self.global_loss_window, now_ns)
            >= self.global_loss_suspect_threshold
        {
            if !self.global_kill_switch.swap(true, Ordering::Relaxed) {
                out.global_killed = true;
                for idx in 0..self.shards.len() {
                    self.kill_shard(idx, now_ns);
                }
            }
        }

        out
    }

    fn record_session_loss_and_count(&self, session_id: &str, now_ns: u64) -> usize {
        let key = session_id.to_string();
        let window = if let Some(existing) = self.session_loss_windows.get(&key) {
            existing.clone()
        } else {
            let inserted = Arc::new(std::sync::Mutex::new(VecDeque::new()));
            self.session_loss_windows
                .entry(key)
                .or_insert_with(|| Arc::clone(&inserted))
                .clone()
        };
        self.record_loss_and_count(&window, now_ns)
    }

    pub(super) fn kill_shard_due_to_watermark(&self, shard_id: usize, now_ns: u64) -> bool {
        self.kill_shard(shard_id, now_ns)
    }

    fn record_loss_and_count(
        &self,
        window: &std::sync::Mutex<VecDeque<u64>>,
        now_ns: u64,
    ) -> usize {
        let mut guard = match window.lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.push_back(now_ns);
        let horizon = self.loss_window_ns;
        while let Some(front) = guard.front().copied() {
            if now_ns.saturating_sub(front) <= horizon {
                break;
            }
            guard.pop_front();
        }
        guard.len()
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
    pub(super) v2_durable_wait_timeout_ms: u64,
    pub(super) v2_requests_total: Arc<AtomicU64>,
    pub(super) v2_durable_wait_timeout_total: Arc<AtomicU64>,
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
    pub(super) v3_rejected_hard_total: Arc<AtomicU64>,
    pub(super) v3_rejected_killed_total: Arc<AtomicU64>,
    pub(super) v3_kill_recovered_total: Arc<AtomicU64>,
    pub(super) v3_loss_suspect_total: Arc<AtomicU64>,
    pub(super) v3_session_killed_total: Arc<AtomicU64>,
    pub(super) v3_shard_killed_total: Arc<AtomicU64>,
    pub(super) v3_global_killed_total: Arc<AtomicU64>,
    pub(super) v3_durable_accepted_total: Arc<AtomicU64>,
    pub(super) v3_durable_rejected_total: Arc<AtomicU64>,
    pub(super) v3_live_ack_hist: Arc<LatencyHistogram>,
    pub(super) v3_live_ack_accepted_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_confirm_hist: Arc<LatencyHistogram>,
    pub(super) v3_soft_reject_pct: u64,
    pub(super) v3_hard_reject_pct: u64,
    pub(super) v3_kill_reject_pct: u64,
    pub(super) v3_confirm_store: Arc<V3ConfirmStore>,
    pub(super) v3_loss_gap_timeout_ms: u64,
    pub(super) v3_loss_scan_interval_ms: u64,
    pub(super) v3_loss_scan_batch: usize,
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

impl AppState {
    pub(super) fn register_v3_loss_suspect(
        &self,
        session_id: &str,
        session_seq: u64,
        shard_id: usize,
        reason: &str,
        now_ns: u64,
    ) {
        let Some(marked_shard) = self.v3_confirm_store.mark_loss_suspect(
            session_id,
            session_seq,
            shard_id,
            reason,
            now_ns,
        ) else {
            return;
        };
        self.v3_loss_suspect_total.fetch_add(1, Ordering::Relaxed);
        let shard_count = self.v3_ingress.shard_count().max(1);
        let mut effective_shard = marked_shard;
        if effective_shard >= shard_count {
            effective_shard = shard_id.min(shard_count - 1);
        }
        let escalated = self
            .v3_ingress
            .mark_loss_suspect(session_id, effective_shard, now_ns);
        if escalated.session_killed {
            self.v3_session_killed_total.fetch_add(1, Ordering::Relaxed);
        }
        if escalated.shard_killed {
            self.v3_shard_killed_total.fetch_add(1, Ordering::Relaxed);
        }
        if escalated.global_killed {
            self.v3_global_killed_total.fetch_add(1, Ordering::Relaxed);
        }
    }
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

fn parse_v3_symbol_limits(
    default_max_order_qty: u64,
    default_max_notional: u64,
) -> HashMap<[u8; 8], SymbolLimits> {
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
    let v3_kill_auto_recover = parse_bool_env("V3_KILL_AUTO_RECOVER").unwrap_or(false);
    let v3_soft_reject_pct = std::env::var("V3_SOFT_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(99))
        .unwrap_or(70);
    let mut v3_hard_reject_pct = std::env::var("V3_HARD_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(99))
        .unwrap_or(85);
    if v3_hard_reject_pct <= v3_soft_reject_pct {
        v3_hard_reject_pct = (v3_soft_reject_pct + 1).min(99);
    }
    let mut v3_kill_reject_pct = std::env::var("V3_KILL_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(100))
        .unwrap_or(95);
    if v3_kill_reject_pct <= v3_hard_reject_pct {
        v3_kill_reject_pct = (v3_hard_reject_pct + 1).min(100);
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
    let v3_shard_count = std::env::var("V3_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4);
    let v3_durable_queue_capacity = std::env::var("V3_DURABILITY_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(200_000);
    let v3_loss_gap_timeout_ms = std::env::var("V3_LOSS_GAP_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(500);
    let v3_loss_scan_interval_ms = std::env::var("V3_LOSS_SCAN_INTERVAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(50);
    let v3_loss_scan_batch = std::env::var("V3_LOSS_SCAN_BATCH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(2_048);
    let v3_loss_window_sec = std::env::var("V3_LOSS_WINDOW_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(60);
    let v3_session_loss_suspect_threshold =
        std::env::var("V3_SESSION_LOSS_SUSPECT_THRESHOLD")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(1);
    let v3_shard_loss_suspect_threshold = std::env::var("V3_SHARD_LOSS_SUSPECT_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(3);
    let v3_global_loss_suspect_threshold = std::env::var("V3_GLOBAL_LOSS_SUSPECT_THRESHOLD")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(6);
    let v2_durable_wait_timeout_ms = std::env::var("V2_DURABLE_WAIT_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(200);
    let mut v3_shards = Vec::with_capacity(v3_shard_count);
    let mut v3_rxs = Vec::with_capacity(v3_shard_count);
    for _ in 0..v3_shard_count {
        let (tx, rx) = tokio::sync::mpsc::channel(v3_queue_capacity);
        v3_shards.push(V3ShardIngress::new(tx, v3_queue_capacity as u64));
        v3_rxs.push(rx);
    }
    let v3_ingress = Arc::new(V3Ingress::new(
        v3_shards,
        v3_kill_auto_recover,
        v3_kill_recover_pct,
        v3_kill_recover_after_ms,
        v3_loss_window_sec,
        v3_session_loss_suspect_threshold,
        v3_shard_loss_suspect_threshold,
        v3_global_loss_suspect_threshold,
    ));
    let (v3_durable_tx, v3_durable_rx) = tokio::sync::mpsc::channel(v3_durable_queue_capacity);

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
        v2_durable_wait_timeout_ms,
        v2_requests_total: Arc::new(AtomicU64::new(0)),
        v2_durable_wait_timeout_total: Arc::new(AtomicU64::new(0)),
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
        v3_rejected_hard_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_killed_total: Arc::new(AtomicU64::new(0)),
        v3_kill_recovered_total: Arc::new(AtomicU64::new(0)),
        v3_loss_suspect_total: Arc::new(AtomicU64::new(0)),
        v3_session_killed_total: Arc::new(AtomicU64::new(0)),
        v3_shard_killed_total: Arc::new(AtomicU64::new(0)),
        v3_global_killed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_accepted_total: Arc::new(AtomicU64::new(0)),
        v3_durable_rejected_total: Arc::new(AtomicU64::new(0)),
        v3_live_ack_hist: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_confirm_hist: Arc::new(LatencyHistogram::new()),
        v3_soft_reject_pct,
        v3_hard_reject_pct,
        v3_kill_reject_pct,
        v3_confirm_store: Arc::new(V3ConfirmStore::new()),
        v3_loss_gap_timeout_ms,
        v3_loss_scan_interval_ms,
        v3_loss_scan_batch,
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

    for (shard_id, v3_rx) in v3_rxs.into_iter().enumerate() {
        let writer_state = state.clone();
        let durable_tx = v3_durable_tx.clone();
        tokio::spawn(async move {
            run_v3_single_writer(shard_id, v3_rx, writer_state, durable_tx).await;
        });
    }
    drop(v3_durable_tx);

    let durable_state = state.clone();
    tokio::spawn(async move {
        run_v3_durable_worker(v3_durable_rx, durable_state).await;
    });

    let loss_state = state.clone();
    tokio::spawn(async move {
        run_v3_loss_monitor(loss_state).await;
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
        .route(
            "/v3/orders/{session_id}/{session_seq}",
            get(handle_get_order_v3),
        )
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
/// ホットキューを直列消費し、durable confirm 経路へ渡す。
async fn run_v3_single_writer(
    shard_id: usize,
    mut rx: Receiver<V3OrderTask>,
    state: AppState,
    durable_tx: Sender<V3DurableTask>,
) {
    while let Some(task) = rx.recv().await {
        state.v3_ingress.on_processed_one(shard_id);
        state
            .v3_confirm_store
            .record_volatile(&task, gateway_core::now_nanos());
        match durable_tx.try_send(task.into()) {
            Ok(()) => {}
            Err(TrySendError::Full(task)) => {
                state.register_v3_loss_suspect(
                    &task.session_id,
                    task.session_seq,
                    task.shard_id,
                    "DURABILITY_QUEUE_FULL",
                    gateway_core::now_nanos(),
                );
            }
            Err(TrySendError::Closed(task)) => {
                state.register_v3_loss_suspect(
                    &task.session_id,
                    task.session_seq,
                    task.shard_id,
                    "DURABILITY_QUEUE_CLOSED",
                    gateway_core::now_nanos(),
                );
            }
        }
    }
}

/// /v3 の durable confirm worker。
/// hot path外で WAL durable を実行し、結果を status store に反映する。
async fn run_v3_durable_worker(mut rx: Receiver<V3DurableTask>, state: AppState) {
    while let Some(task) = rx.recv().await {
        let event = AuditEvent {
            event_type: "V3DurableAccepted".to_string(),
            at: crate::audit::now_millis(),
            account_id: task.session_id.clone(),
            order_id: Some(format!("v3/{}/{}", task.session_id, task.session_seq)),
            data: serde_json::json!({
                "sessionSeq": task.session_seq,
                "attemptId": format!("att_{}", task.attempt_seq),
            }),
        };
        let timings = state.audit_log.append_durable_with_timings(event);
        let now_ns = gateway_core::now_nanos();
        if timings.durable_done_ns > 0 {
            state.v3_confirm_store.mark_durable_accepted(
                &task.session_id,
                task.session_seq,
                now_ns,
            );
            state
                .v3_durable_accepted_total
                .fetch_add(1, Ordering::Relaxed);
        } else {
            state.v3_confirm_store.mark_durable_rejected(
                &task.session_id,
                task.session_seq,
                "WAL_DURABILITY_FAILED",
                now_ns,
            );
            state
                .v3_durable_rejected_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_us = now_ns.saturating_sub(task.received_at_ns) / 1_000;
        state.v3_durable_confirm_hist.record(elapsed_us);
    }
}

/// /v3 の未確定注文を監視し、timeout 時は LOSS_SUSPECT へ昇格する。
async fn run_v3_loss_monitor(state: AppState) {
    let interval_ms = state.v3_loss_scan_interval_ms.max(10);
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    loop {
        ticker.tick().await;
        let now_ns = gateway_core::now_nanos();
        let timeout_ns = state.v3_loss_gap_timeout_ms.saturating_mul(1_000_000);
        let candidates =
            state
                .v3_confirm_store
                .collect_timed_out(now_ns, timeout_ns, state.v3_loss_scan_batch);
        for candidate in candidates {
            state.register_v3_loss_suspect(
                &candidate.session_id,
                candidate.session_seq,
                candidate.shard_id,
                "DURABLE_CONFIRM_TIMEOUT",
                now_ns,
            );
        }

        if !state.v3_ingress.is_global_killed() {
            for shard_id in 0..state.v3_ingress.shard_count() {
                if state.v3_ingress.maybe_recover_shard(shard_id, now_ns) {
                    state
                        .v3_kill_recovered_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }
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
                if state
                    .sharded_store
                    .mark_durable(order_id, &note.account_id, note.event_at)
                {
                    state.inflight_controller.on_commit(1);
                    state
                        .sse_hub
                        .publish_order(order_id, "order_durable", &data);
                    state
                        .sse_hub
                        .publish_account(&note.account_id, "order_durable", &data);
                }
            }
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
