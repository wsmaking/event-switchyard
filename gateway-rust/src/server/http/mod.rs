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
    http::StatusCode,
    routing::{get, post},
};
use gateway_core::SymbolLimits;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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
use std::path::{Path, PathBuf};

use audit::{handle_account_events, handle_audit_anchor, handle_audit_verify, handle_order_events};
use metrics::{handle_health, handle_metrics};
use orders::{
    V3_TCP_REQUEST_SIZE, authenticate_v3_tcp_token, decode_v3_tcp_request,
    encode_v3_tcp_decode_error, encode_v3_tcp_response, handle_cancel_order, handle_get_order,
    handle_get_order_by_client_id, handle_get_order_v2, handle_get_order_v3, handle_order,
    handle_order_v2, handle_order_v3, process_order_v3_hot_path,
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

#[derive(Clone)]
struct V3DurableLane {
    tx: Sender<V3DurableTask>,
    depth: Arc<AtomicU64>,
    capacity: u64,
    processed_total: Arc<AtomicU64>,
    queue_full_total: Arc<AtomicU64>,
    queue_closed_total: Arc<AtomicU64>,
}

impl V3DurableLane {
    fn new(tx: Sender<V3DurableTask>, capacity: u64) -> Self {
        Self {
            tx,
            depth: Arc::new(AtomicU64::new(0)),
            capacity,
            processed_total: Arc::new(AtomicU64::new(0)),
            queue_full_total: Arc::new(AtomicU64::new(0)),
            queue_closed_total: Arc::new(AtomicU64::new(0)),
        }
    }
}

#[derive(Clone)]
pub(super) struct V3DurableIngress {
    lanes: Arc<Vec<V3DurableLane>>,
}

impl V3DurableIngress {
    fn new(lanes: Vec<V3DurableLane>) -> Self {
        Self {
            lanes: Arc::new(lanes),
        }
    }

    pub(super) fn lane_count(&self) -> usize {
        self.lanes.len().max(1)
    }

    pub(super) fn lane_for_shard(&self, shard_id: usize) -> usize {
        shard_id % self.lane_count()
    }

    fn try_enqueue(&self, task: V3DurableTask) -> Result<(), TrySendError<V3DurableTask>> {
        // durable lane は ingress shard と同じ分割キーで固定し、順序と局所性を揃える。
        let lane_id = self.lane_for_shard(task.shard_id);
        let Some(lane) = self.lanes.get(lane_id) else {
            return Err(TrySendError::Closed(task));
        };
        match lane.tx.try_send(task) {
            Ok(()) => {
                lane.depth.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(TrySendError::Full(task)) => {
                lane.queue_full_total.fetch_add(1, Ordering::Relaxed);
                Err(TrySendError::Full(task))
            }
            Err(TrySendError::Closed(task)) => {
                lane.queue_closed_total.fetch_add(1, Ordering::Relaxed);
                Err(TrySendError::Closed(task))
            }
        }
    }

    fn on_processed_one(&self, lane_id: usize) {
        let Some(lane) = self.lanes.get(lane_id) else {
            return;
        };
        let prev = lane.depth.fetch_sub(1, Ordering::Relaxed);
        if prev == 0 {
            lane.depth.store(0, Ordering::Relaxed);
        }
        lane.processed_total.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn total_depth(&self) -> u64 {
        self.lanes
            .iter()
            .map(|lane| lane.depth.load(Ordering::Relaxed))
            .sum()
    }

    pub(super) fn total_capacity(&self) -> u64 {
        self.lanes.iter().map(|lane| lane.capacity).sum()
    }

    pub(super) fn queue_utilization_pct_max(&self) -> f64 {
        self.lanes
            .iter()
            .map(|lane| {
                if lane.capacity == 0 {
                    100.0
                } else {
                    lane.depth.load(Ordering::Relaxed) as f64 * 100.0 / lane.capacity as f64
                }
            })
            .fold(0.0, f64::max)
    }

    pub(super) fn lane_utilization_pct(&self, lane_id: usize) -> f64 {
        let Some(lane) = self.lanes.get(lane_id) else {
            return 100.0;
        };
        if lane.capacity == 0 {
            100.0
        } else {
            lane.depth.load(Ordering::Relaxed) as f64 * 100.0 / lane.capacity as f64
        }
    }

    pub(super) fn lane_depth(&self, lane_id: usize) -> u64 {
        self.lanes
            .get(lane_id)
            .map(|lane| lane.depth.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    pub(super) fn lane_skew_pct(&self) -> f64 {
        let mut max = 0u64;
        let mut total = 0u64;
        for lane in self.lanes.iter() {
            let depth = lane.depth.load(Ordering::Relaxed);
            max = max.max(depth);
            total = total.saturating_add(depth);
        }
        if total == 0 {
            return 0.0;
        }
        let avg = total as f64 / self.lane_count() as f64;
        if avg <= 0.0 {
            return 0.0;
        }
        ((max as f64 / avg) - 1.0) * 100.0
    }

    pub(super) fn queue_full_total(&self) -> u64 {
        self.lanes
            .iter()
            .map(|lane| lane.queue_full_total.load(Ordering::Relaxed))
            .sum()
    }

    pub(super) fn queue_closed_total(&self) -> u64 {
        self.lanes
            .iter()
            .map(|lane| lane.queue_closed_total.load(Ordering::Relaxed))
            .sum()
    }

    pub(super) fn processed_total(&self) -> u64 {
        self.lanes
            .iter()
            .map(|lane| lane.processed_total.load(Ordering::Relaxed))
            .sum()
    }

    pub(super) fn lane_depths(&self) -> Vec<u64> {
        self.lanes
            .iter()
            .map(|lane| lane.depth.load(Ordering::Relaxed))
            .collect()
    }

    pub(super) fn lane_utilization_pcts(&self) -> Vec<f64> {
        self.lanes
            .iter()
            .map(|lane| {
                if lane.capacity == 0 {
                    100.0
                } else {
                    lane.depth.load(Ordering::Relaxed) as f64 * 100.0 / lane.capacity as f64
                }
            })
            .collect()
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct V3DeadlineEntry {
    deadline_ns: u64,
    session_id: String,
    session_seq: u64,
}

impl Ord for V3DeadlineEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse ordering for min-heap semantics (earliest deadline first).
        other
            .deadline_ns
            .cmp(&self.deadline_ns)
            .then_with(|| other.session_seq.cmp(&self.session_seq))
            .then_with(|| other.session_id.cmp(&self.session_id))
    }
}

impl PartialOrd for V3DeadlineEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Default)]
pub(super) struct V3TimeoutScanResult {
    pub(super) candidates: Vec<V3LossCandidate>,
    pub(super) scan_cost: u64,
}

#[derive(Clone)]
pub(super) struct V3ConfirmStore {
    records: Arc<Vec<Arc<dashmap::DashMap<(String, u64), V3ConfirmRecord>>>>,
    timeout_wheels: Arc<Vec<Arc<std::sync::Mutex<BinaryHeap<V3DeadlineEntry>>>>>,
    gc_wheels: Arc<Vec<Arc<std::sync::Mutex<BinaryHeap<V3DeadlineEntry>>>>>,
    timeout_ns: u64,
    ttl_ns: u64,
}

impl V3ConfirmStore {
    pub(super) fn new(lane_count: usize, timeout_ms: u64, ttl_ms: u64) -> Self {
        let lanes = lane_count.max(1);
        let timeout_ns = timeout_ms.max(1).saturating_mul(1_000_000);
        let ttl_ns = ttl_ms.max(timeout_ms.max(1)).saturating_mul(1_000_000);
        let mut records = Vec::with_capacity(lanes);
        let mut timeout_wheels = Vec::with_capacity(lanes);
        let mut gc_wheels = Vec::with_capacity(lanes);
        for _ in 0..lanes {
            records.push(Arc::new(dashmap::DashMap::new()));
            timeout_wheels.push(Arc::new(std::sync::Mutex::new(BinaryHeap::new())));
            gc_wheels.push(Arc::new(std::sync::Mutex::new(BinaryHeap::new())));
        }
        Self {
            records: Arc::new(records),
            timeout_wheels: Arc::new(timeout_wheels),
            gc_wheels: Arc::new(gc_wheels),
            timeout_ns,
            ttl_ns,
        }
    }

    fn lane_count(&self) -> usize {
        self.records.len().max(1)
    }

    fn lane_for_session(&self, session_id: &str) -> usize {
        index_for_key(session_id, self.lane_count())
    }

    fn schedule_timeout(
        &self,
        lane: usize,
        session_id: &str,
        session_seq: u64,
        received_at_ns: u64,
    ) {
        let deadline_ns = received_at_ns.saturating_add(self.timeout_ns);
        let mut wheel = match self.timeout_wheels[lane].lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        wheel.push(V3DeadlineEntry {
            deadline_ns,
            session_id: session_id.to_string(),
            session_seq,
        });
    }

    fn schedule_gc(&self, lane: usize, session_id: &str, session_seq: u64, updated_at_ns: u64) {
        let deadline_ns = updated_at_ns.saturating_add(self.ttl_ns);
        let mut wheel = match self.gc_wheels[lane].lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        wheel.push(V3DeadlineEntry {
            deadline_ns,
            session_id: session_id.to_string(),
            session_seq,
        });
    }

    pub(super) fn lane_count_metric(&self) -> u64 {
        self.lane_count() as u64
    }

    pub(super) fn total_size(&self) -> u64 {
        self.records.iter().map(|lane| lane.len() as u64).sum()
    }

    pub(super) fn lane_skew_pct(&self) -> f64 {
        let mut max = 0u64;
        let mut total = 0u64;
        for lane in self.records.iter() {
            let size = lane.len() as u64;
            max = max.max(size);
            total = total.saturating_add(size);
        }
        if total == 0 {
            return 0.0;
        }
        let avg = total as f64 / self.lane_count() as f64;
        if avg <= 0.0 {
            return 0.0;
        }
        ((max as f64 / avg) - 1.0) * 100.0
    }

    pub(super) fn oldest_volatile_age_us_per_lane(&self, now_ns: u64) -> Vec<u64> {
        let mut out = vec![0u64; self.lane_count()];
        for lane in 0..self.lane_count() {
            let deadline_ns_opt = {
                let mut wheel = match self.timeout_wheels[lane].lock() {
                    Ok(v) => v,
                    Err(poisoned) => poisoned.into_inner(),
                };
                loop {
                    let Some(peek) = wheel.peek() else {
                        break None;
                    };
                    let key = (peek.session_id.clone(), peek.session_seq);
                    let is_volatile = self.records[lane]
                        .get(&key)
                        .map(|entry| entry.status == V3ConfirmStatus::VolatileAccept)
                        .unwrap_or(false);
                    if is_volatile {
                        break Some(peek.deadline_ns);
                    }
                    wheel.pop();
                }
            };
            if let Some(deadline_ns) = deadline_ns_opt {
                let received_at_ns = deadline_ns.saturating_sub(self.timeout_ns);
                out[lane] = now_ns.saturating_sub(received_at_ns) / 1_000;
            }
        }
        out
    }

    pub(super) fn record_volatile(&self, task: &V3OrderTask, now_ns: u64) {
        let lane = self.lane_for_session(&task.session_id);
        self.records[lane].insert(
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
        self.schedule_timeout(
            lane,
            &task.session_id,
            task.session_seq,
            task.received_at_ns,
        );
        self.schedule_gc(lane, &task.session_id, task.session_seq, now_ns);
    }

    pub(super) fn mark_durable_accepted(&self, session_id: &str, session_seq: u64, now_ns: u64) {
        let lane = self.lane_for_session(session_id);
        if let Some(mut entry) = self.records[lane].get_mut(&(session_id.to_string(), session_seq))
        {
            entry.status = V3ConfirmStatus::DurableAccepted;
            entry.reason = None;
            entry.updated_at_ns = now_ns;
            self.schedule_gc(lane, session_id, session_seq, now_ns);
            return;
        }
        self.records[lane].insert(
            (session_id.to_string(), session_seq),
            V3ConfirmRecord {
                status: V3ConfirmStatus::DurableAccepted,
                reason: None,
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id: lane,
            },
        );
        self.schedule_gc(lane, session_id, session_seq, now_ns);
    }

    pub(super) fn mark_durable_rejected(
        &self,
        session_id: &str,
        session_seq: u64,
        reason: &str,
        now_ns: u64,
    ) {
        let lane = self.lane_for_session(session_id);
        if let Some(mut entry) = self.records[lane].get_mut(&(session_id.to_string(), session_seq))
        {
            entry.status = V3ConfirmStatus::DurableRejected;
            entry.reason = Some(reason.to_string());
            entry.updated_at_ns = now_ns;
            self.schedule_gc(lane, session_id, session_seq, now_ns);
            return;
        }
        self.records[lane].insert(
            (session_id.to_string(), session_seq),
            V3ConfirmRecord {
                status: V3ConfirmStatus::DurableRejected,
                reason: Some(reason.to_string()),
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id: lane,
            },
        );
        self.schedule_gc(lane, session_id, session_seq, now_ns);
    }

    pub(super) fn mark_loss_suspect(
        &self,
        session_id: &str,
        session_seq: u64,
        shard_id: usize,
        reason: &str,
        now_ns: u64,
    ) -> Option<usize> {
        let lane = self.lane_for_session(session_id);
        if let Some(mut entry) = self.records[lane].get_mut(&(session_id.to_string(), session_seq))
        {
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
            self.schedule_gc(lane, session_id, session_seq, now_ns);
            return Some(entry.shard_id);
        }
        self.records[lane].insert(
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
        self.schedule_gc(lane, session_id, session_seq, now_ns);
        Some(shard_id)
    }

    pub(super) fn snapshot(&self, session_id: &str, session_seq: u64) -> Option<V3ConfirmSnapshot> {
        let lane = self.lane_for_session(session_id);
        self.records[lane]
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

    pub(super) fn collect_timed_out(&self, now_ns: u64, max_scan: usize) -> V3TimeoutScanResult {
        let mut out = Vec::new();
        let mut scan_cost = 0u64;
        if max_scan == 0 {
            return V3TimeoutScanResult {
                candidates: out,
                scan_cost,
            };
        }
        for lane in 0..self.lane_count() {
            while out.len() < max_scan {
                let entry = {
                    let mut wheel = match self.timeout_wheels[lane].lock() {
                        Ok(v) => v,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    let Some(peek) = wheel.peek() else {
                        break;
                    };
                    if peek.deadline_ns > now_ns {
                        break;
                    }
                    wheel.pop()
                };
                let Some(entry) = entry else {
                    break;
                };
                scan_cost = scan_cost.saturating_add(1);
                let key = (entry.session_id.clone(), entry.session_seq);
                if let Some(item) = self.records[lane].get(&key) {
                    if item.status != V3ConfirmStatus::VolatileAccept {
                        continue;
                    }
                    if now_ns < entry.deadline_ns {
                        continue;
                    }
                    out.push(V3LossCandidate {
                        session_id: key.0,
                        session_seq: key.1,
                        shard_id: item.shard_id,
                    });
                }
            }
            if out.len() >= max_scan {
                break;
            }
        }
        V3TimeoutScanResult {
            candidates: out,
            scan_cost,
        }
    }

    pub(super) fn gc_expired(&self, now_ns: u64, max_scan: usize) -> usize {
        let mut removed = 0usize;
        if max_scan == 0 {
            return removed;
        }
        for lane in 0..self.lane_count() {
            while removed < max_scan {
                let entry = {
                    let mut wheel = match self.gc_wheels[lane].lock() {
                        Ok(v) => v,
                        Err(poisoned) => poisoned.into_inner(),
                    };
                    let Some(peek) = wheel.peek() else {
                        break;
                    };
                    if peek.deadline_ns > now_ns {
                        break;
                    }
                    wheel.pop()
                };
                let Some(entry) = entry else {
                    break;
                };
                let key = (entry.session_id, entry.session_seq);
                if let Some(item) = self.records[lane].get(&key) {
                    if item.updated_at_ns.saturating_add(self.ttl_ns) > now_ns {
                        continue;
                    }
                } else {
                    continue;
                }
                self.records[lane].remove(&key);
                removed = removed.saturating_add(1);
            }
            if removed >= max_scan {
                break;
            }
        }
        removed
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
    session_shard: Arc<dashmap::DashMap<String, usize>>,
    shard_session_counts: Arc<Vec<Arc<AtomicU64>>>,
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
        let mut session_counts = Vec::with_capacity(shard_len);
        for _ in 0..shard_len {
            windows.push(Arc::new(std::sync::Mutex::new(VecDeque::new())));
            session_counts.push(Arc::new(AtomicU64::new(0)));
        }
        Self {
            shards: Arc::new(shards),
            session_seq: Arc::new(dashmap::DashMap::new()),
            session_shard: Arc::new(dashmap::DashMap::new()),
            shard_session_counts: Arc::new(session_counts),
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

    fn choose_shard_for_new_session(&self) -> usize {
        let mut best_idx = 0usize;
        let mut best_sessions = u64::MAX;
        let mut best_depth = u64::MAX;
        for (idx, shard) in self.shards.iter().enumerate() {
            let sessions = self
                .shard_session_counts
                .get(idx)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0);
            let depth = shard.depth.load(Ordering::Relaxed);
            if sessions < best_sessions || (sessions == best_sessions && depth < best_depth) {
                best_idx = idx;
                best_sessions = sessions;
                best_depth = depth;
            }
        }
        best_idx
    }

    fn shard_index_for_session(&self, session_id: &str) -> usize {
        use dashmap::mapref::entry::Entry;
        match self.session_shard.entry(session_id.to_string()) {
            Entry::Occupied(v) => *v.get(),
            Entry::Vacant(v) => {
                let shard_id = self.choose_shard_for_new_session();
                if let Some(counter) = self.shard_session_counts.get(shard_id) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                *v.insert(shard_id)
            }
        }
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
        if self.record_session_loss_and_count(session_id, now_ns)
            >= self.session_loss_suspect_threshold
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
    pub(super) v3_durable_audit_logs: Arc<Vec<Arc<AuditLog>>>,
    pub(super) audit_read_path: Arc<PathBuf>,
    pub(super) v3_confirm_rebuild_paths: Arc<Vec<PathBuf>>,
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
    pub(super) v3_durable_ingress: Arc<V3DurableIngress>,
    pub(super) v3_durable_accepted_total: Arc<AtomicU64>,
    pub(super) v3_durable_rejected_total: Arc<AtomicU64>,
    pub(super) v3_live_ack_hist: Arc<LatencyHistogram>,
    pub(super) v3_live_ack_accepted_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_confirm_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_wal_append_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_wal_fsync_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_wal_fsync_hist_per_lane: Arc<Vec<Arc<LatencyHistogram>>>,
    pub(super) v3_durable_worker_loop_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_worker_loop_hist_per_lane: Arc<Vec<Arc<LatencyHistogram>>>,
    pub(super) v3_durable_worker_batch_min: usize,
    pub(super) v3_durable_worker_batch_max: usize,
    pub(super) v3_durable_worker_batch_wait_min_us: u64,
    pub(super) v3_durable_worker_batch_wait_us: u64,
    pub(super) v3_durable_worker_batch_adaptive: bool,
    pub(super) v3_durable_worker_batch_adaptive_low_util_pct: f64,
    pub(super) v3_durable_worker_batch_adaptive_high_util_pct: f64,
    pub(super) v3_durable_depth_last: Arc<AtomicU64>,
    pub(super) v3_durable_depth_last_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_backlog_growth_per_sec: Arc<AtomicI64>,
    pub(super) v3_durable_backlog_growth_per_sec_per_lane: Arc<Vec<Arc<AtomicI64>>>,
    pub(super) v3_durable_soft_reject_pct: u64,
    pub(super) v3_durable_hard_reject_pct: u64,
    pub(super) v3_durable_backlog_soft_reject_per_sec: i64,
    pub(super) v3_durable_backlog_hard_reject_per_sec: i64,
    pub(super) v3_durable_backlog_signal_min_queue_pct: f64,
    pub(super) v3_durable_admission_controller_enabled: bool,
    pub(super) v3_durable_admission_sustain_ticks: u64,
    pub(super) v3_durable_admission_recover_ticks: u64,
    pub(super) v3_durable_admission_soft_fsync_p99_us: u64,
    pub(super) v3_durable_admission_hard_fsync_p99_us: u64,
    pub(super) v3_durable_admission_fsync_presignal_pct: f64,
    pub(super) v3_durable_admission_level: Arc<AtomicU64>,
    pub(super) v3_durable_admission_level_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_soft_trip_total: Arc<AtomicU64>,
    pub(super) v3_durable_admission_soft_trip_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_hard_trip_total: Arc<AtomicU64>,
    pub(super) v3_durable_admission_hard_trip_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_signal_queue_soft_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_signal_queue_hard_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_signal_backlog_soft_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_signal_backlog_hard_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_signal_fsync_soft_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_admission_signal_fsync_hard_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_backpressure_soft_total: Arc<AtomicU64>,
    pub(super) v3_durable_backpressure_soft_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_backpressure_hard_total: Arc<AtomicU64>,
    pub(super) v3_durable_backpressure_hard_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_write_error_total: Arc<AtomicU64>,
    pub(super) v3_soft_reject_pct: u64,
    pub(super) v3_hard_reject_pct: u64,
    pub(super) v3_kill_reject_pct: u64,
    pub(super) v3_confirm_store: Arc<V3ConfirmStore>,
    pub(super) v3_confirm_oldest_inflight_us: Arc<AtomicU64>,
    pub(super) v3_confirm_oldest_inflight_us_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_confirm_age_hist_per_lane: Arc<Vec<Arc<LatencyHistogram>>>,
    pub(super) v3_loss_gap_timeout_ms: u64,
    pub(super) v3_loss_scan_interval_ms: u64,
    pub(super) v3_loss_scan_batch: usize,
    pub(super) v3_confirm_gc_batch: usize,
    pub(super) v3_confirm_timeout_scan_cost_last: Arc<AtomicU64>,
    pub(super) v3_confirm_timeout_scan_cost_total: Arc<AtomicU64>,
    pub(super) v3_confirm_gc_removed_total: Arc<AtomicU64>,
    pub(super) v3_confirm_rebuild_restored_total: Arc<AtomicU64>,
    pub(super) v3_confirm_rebuild_elapsed_ms: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_soft_reject_age_us: u64,
    pub(super) v3_durable_confirm_hard_reject_age_us: u64,
    pub(super) v3_durable_confirm_age_soft_reject_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_hard_reject_total: Arc<AtomicU64>,
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

fn mix_u64(mut x: u64) -> u64 {
    // MurmurHash3 finalizer mix to improve low-bit spread before bucket selection.
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    x
}

fn jump_consistent_hash(key: u64, buckets: usize) -> usize {
    let buckets = buckets.max(1);
    let mut k = key;
    let mut b = -1i64;
    let mut j = 0i64;
    while j < buckets as i64 {
        b = j;
        k = k.wrapping_mul(2862933555777941757).wrapping_add(1);
        let denom = ((k >> 33) + 1) as f64;
        j = ((b as f64 + 1.0) * ((1u64 << 31) as f64 / denom)) as i64;
    }
    b.max(0) as usize
}

fn index_for_key(key: &str, buckets: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    let mixed = mix_u64(hasher.finish());
    jump_consistent_hash(mixed, buckets)
}

fn parse_v3_order_id(order_id: &str) -> Option<(String, u64)> {
    let rest = order_id.strip_prefix("v3/")?;
    let (session_id, seq_raw) = rest.rsplit_once('/')?;
    let session_seq = seq_raw.parse::<u64>().ok()?;
    if session_id.is_empty() {
        return None;
    }
    Some((session_id.to_string(), session_seq))
}

fn v3_durable_lane_wal_path(base: &Path, lane_id: usize) -> PathBuf {
    PathBuf::from(format!("{}.v3.lane{}.log", base.display(), lane_id))
}

fn rebuild_v3_confirm_store_from_reader<R: BufRead>(
    confirm_store: &V3ConfirmStore,
    reader: R,
    max_lines: usize,
) -> u64 {
    let max_lines = max_lines.clamp(1, 5_000_000);
    let mut ring: VecDeque<AuditEvent> = VecDeque::with_capacity(max_lines.min(16_384));

    for line in reader.lines().flatten() {
        if line.trim().is_empty() {
            continue;
        }
        let event: AuditEvent = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        ring.push_back(event);
        while ring.len() > max_lines {
            ring.pop_front();
        }
    }

    let mut restored = 0u64;
    for event in ring {
        let Some(order_id) = event.order_id.as_deref() else {
            continue;
        };
        let Some((session_id, session_seq)) = parse_v3_order_id(order_id) else {
            continue;
        };
        let at_ns = event.at.saturating_mul(1_000_000);
        match event.event_type.as_str() {
            "V3DurableAccepted" => {
                confirm_store.mark_durable_accepted(&session_id, session_seq, at_ns);
                restored = restored.saturating_add(1);
            }
            "V3DurableRejected" => {
                let reason = event
                    .data
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("WAL_DURABILITY_FAILED");
                confirm_store.mark_durable_rejected(&session_id, session_seq, reason, at_ns);
                restored = restored.saturating_add(1);
            }
            _ => {}
        }
    }

    restored
}

fn rebuild_v3_confirm_store_from_wal_path(state: &AppState, path: &Path, max_lines: usize) -> u64 {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    let reader = BufReader::new(file);
    rebuild_v3_confirm_store_from_reader(&state.v3_confirm_store, reader, max_lines)
}

fn rebuild_v3_confirm_store_from_wal(state: &AppState, max_lines: usize) -> u64 {
    state
        .v3_confirm_rebuild_paths
        .iter()
        .map(|path| rebuild_v3_confirm_store_from_wal_path(state, path, max_lines))
        .sum()
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
    let v3_shard_count_default = std::thread::available_parallelism()
        .map(|n| n.get().clamp(4, 8))
        .unwrap_or(4);
    let v3_shard_count = std::env::var("V3_SHARD_COUNT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(v3_shard_count_default);
    let v3_durable_queue_capacity = std::env::var("V3_DURABILITY_QUEUE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(200_000);
    let configured_v3_durable_lane_count = std::env::var("V3_DURABLE_LANE_COUNT")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0);
    if let Some(configured) = configured_v3_durable_lane_count {
        if configured != v3_shard_count {
            info!(
                configured = configured,
                shard_count = v3_shard_count,
                "V3_DURABLE_LANE_COUNT ignored: shard->durable lane is fixed 1:1"
            );
        }
    }
    let v3_durable_lane_count = v3_shard_count;
    let v3_durable_worker_batch_max = std::env::var("V3_DURABLE_WORKER_BATCH_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(12);
    let mut v3_durable_worker_batch_min = std::env::var("V3_DURABLE_WORKER_BATCH_MIN")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or((v3_durable_worker_batch_max / 2).max(1));
    if v3_durable_worker_batch_min > v3_durable_worker_batch_max {
        v3_durable_worker_batch_min = v3_durable_worker_batch_max;
    }
    let v3_durable_worker_batch_wait_us = std::env::var("V3_DURABLE_WORKER_BATCH_WAIT_US")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(80);
    let mut v3_durable_worker_batch_wait_min_us =
        std::env::var("V3_DURABLE_WORKER_BATCH_WAIT_MIN_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or((v3_durable_worker_batch_wait_us / 2).max(20));
    if v3_durable_worker_batch_wait_min_us > v3_durable_worker_batch_wait_us {
        v3_durable_worker_batch_wait_min_us = v3_durable_worker_batch_wait_us.max(1);
    }
    let v3_durable_worker_batch_adaptive =
        parse_bool_env("V3_DURABLE_WORKER_BATCH_ADAPTIVE").unwrap_or(false);
    let mut v3_durable_worker_batch_adaptive_low_util_pct =
        std::env::var("V3_DURABLE_WORKER_BATCH_ADAPTIVE_LOW_UTIL_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(10.0)
            .clamp(0.0, 99.0);
    let mut v3_durable_worker_batch_adaptive_high_util_pct =
        std::env::var("V3_DURABLE_WORKER_BATCH_ADAPTIVE_HIGH_UTIL_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(60.0)
            .clamp(1.0, 100.0);
    if v3_durable_worker_batch_adaptive_high_util_pct
        <= v3_durable_worker_batch_adaptive_low_util_pct
    {
        v3_durable_worker_batch_adaptive_high_util_pct =
            (v3_durable_worker_batch_adaptive_low_util_pct + 1.0).min(100.0);
    }
    if v3_durable_worker_batch_adaptive_low_util_pct
        >= v3_durable_worker_batch_adaptive_high_util_pct
    {
        v3_durable_worker_batch_adaptive_low_util_pct =
            (v3_durable_worker_batch_adaptive_high_util_pct - 1.0).max(0.0);
    }
    let v3_durable_worker_batch_adaptive_cfg = V3DurableWorkerBatchAdaptiveConfig {
        enabled: v3_durable_worker_batch_adaptive,
        batch_min: v3_durable_worker_batch_min,
        batch_max: v3_durable_worker_batch_max,
        wait_min: Duration::from_micros(v3_durable_worker_batch_wait_min_us.max(1)),
        wait_max: Duration::from_micros(v3_durable_worker_batch_wait_us.max(1)),
        low_util_pct: v3_durable_worker_batch_adaptive_low_util_pct,
        high_util_pct: v3_durable_worker_batch_adaptive_high_util_pct,
    };
    let durable_soft_reject_default = v3_soft_reject_pct.saturating_sub(15).max(50);
    let v3_durable_soft_reject_pct = std::env::var("V3_DURABLE_SOFT_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(99))
        .unwrap_or(durable_soft_reject_default);
    let durable_hard_reject_default = v3_hard_reject_pct
        .saturating_sub(10)
        .max(v3_durable_soft_reject_pct.saturating_add(1))
        .min(99);
    let mut v3_durable_hard_reject_pct = std::env::var("V3_DURABLE_HARD_REJECT_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.min(99))
        .unwrap_or(durable_hard_reject_default);
    if v3_durable_hard_reject_pct <= v3_durable_soft_reject_pct {
        v3_durable_hard_reject_pct = (v3_durable_soft_reject_pct + 1).min(99);
    }
    let durable_backlog_soft_reject_default = ((v3_durable_queue_capacity as i64) / 200).max(1_000);
    let v3_durable_backlog_soft_reject_per_sec =
        std::env::var("V3_DURABLE_BACKLOG_SOFT_REJECT_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v >= 0)
            .unwrap_or(durable_backlog_soft_reject_default);
    let durable_backlog_hard_reject_default = durable_backlog_soft_reject_default.saturating_mul(2);
    let mut v3_durable_backlog_hard_reject_per_sec =
        std::env::var("V3_DURABLE_BACKLOG_HARD_REJECT_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .filter(|v| *v >= 0)
            .unwrap_or(durable_backlog_hard_reject_default);
    if v3_durable_backlog_hard_reject_per_sec <= v3_durable_backlog_soft_reject_per_sec {
        v3_durable_backlog_hard_reject_per_sec =
            v3_durable_backlog_soft_reject_per_sec.saturating_add(1);
    }
    let v3_durable_backlog_signal_min_queue_pct =
        std::env::var("V3_DURABLE_BACKLOG_SIGNAL_MIN_QUEUE_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(20.0)
            .clamp(0.0, 100.0);
    let v3_durable_admission_controller_enabled =
        parse_bool_env("V3_DURABLE_ADMISSION_CONTROLLER_ENABLED").unwrap_or(true);
    let v3_durable_admission_sustain_ticks = std::env::var("V3_DURABLE_ADMISSION_SUSTAIN_TICKS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(4);
    let v3_durable_admission_recover_ticks = std::env::var("V3_DURABLE_ADMISSION_RECOVER_TICKS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(8);
    let v3_durable_admission_soft_fsync_p99_us =
        std::env::var("V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(6_000);
    let mut v3_durable_admission_hard_fsync_p99_us =
        std::env::var("V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(12_000);
    if v3_durable_admission_hard_fsync_p99_us <= v3_durable_admission_soft_fsync_p99_us {
        v3_durable_admission_hard_fsync_p99_us =
            v3_durable_admission_soft_fsync_p99_us.saturating_add(1);
    }
    let v3_durable_admission_fsync_presignal_pct =
        std::env::var("V3_DURABLE_ADMISSION_FSYNC_PRESIGNAL_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(1.0)
            .clamp(0.5, 1.0);
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
    let v3_confirm_lanes = std::env::var("V3_CONFIRM_LANES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(v3_shard_count);
    let v3_confirm_ttl_ms = std::env::var("V3_CONFIRM_TTL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(600_000);
    let v3_confirm_gc_batch = std::env::var("V3_CONFIRM_GC_BATCH")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(v3_loss_scan_batch.max(1));
    let v3_durable_confirm_soft_reject_age_us =
        std::env::var("V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
    let mut v3_durable_confirm_hard_reject_age_us =
        std::env::var("V3_DURABLE_CONFIRM_HARD_REJECT_AGE_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
    if v3_durable_confirm_soft_reject_age_us > 0
        && v3_durable_confirm_hard_reject_age_us > 0
        && v3_durable_confirm_hard_reject_age_us <= v3_durable_confirm_soft_reject_age_us
    {
        v3_durable_confirm_hard_reject_age_us =
            v3_durable_confirm_soft_reject_age_us.saturating_add(1);
    }
    let v3_confirm_rebuild_on_start = parse_bool_env("V3_CONFIRM_REBUILD_ON_START").unwrap_or(true);
    let v3_confirm_rebuild_max_lines = std::env::var("V3_CONFIRM_REBUILD_MAX_LINES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(500_000);
    let v3_loss_window_sec = std::env::var("V3_LOSS_WINDOW_SEC")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(60);
    let v3_session_loss_suspect_threshold = std::env::var("V3_SESSION_LOSS_SUSPECT_THRESHOLD")
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
    let per_lane_capacity = (v3_durable_queue_capacity / v3_durable_lane_count.max(1)).max(1);
    let mut v3_durable_lanes = Vec::with_capacity(v3_durable_lane_count);
    let mut v3_durable_rxs = Vec::with_capacity(v3_durable_lane_count);
    for _ in 0..v3_durable_lane_count {
        let (tx, rx) = tokio::sync::mpsc::channel(per_lane_capacity);
        v3_durable_lanes.push(V3DurableLane::new(tx, per_lane_capacity as u64));
        v3_durable_rxs.push(rx);
    }
    let v3_durable_ingress = Arc::new(V3DurableIngress::new(v3_durable_lanes));
    let mut v3_durable_audit_logs = Vec::with_capacity(v3_durable_lane_count);
    let mut v3_confirm_rebuild_paths = Vec::with_capacity(v3_durable_lane_count + 1);
    v3_confirm_rebuild_paths.push(audit_log.path().to_path_buf());
    for lane_id in 0..v3_durable_lane_count {
        let lane_path = v3_durable_lane_wal_path(audit_log.path(), lane_id);
        let lane_log = Arc::new(AuditLog::new(&lane_path)?);
        lane_log.clone().start_async_writer(None);
        v3_confirm_rebuild_paths.push(lane_path);
        v3_durable_audit_logs.push(lane_log);
    }
    let new_lane_u64_counters = || {
        Arc::new(
            (0..v3_durable_lane_count)
                .map(|_| Arc::new(AtomicU64::new(0)))
                .collect::<Vec<_>>(),
        )
    };
    let new_lane_i64_gauges = || {
        Arc::new(
            (0..v3_durable_lane_count)
                .map(|_| Arc::new(AtomicI64::new(0)))
                .collect::<Vec<_>>(),
        )
    };
    let new_confirm_lane_u64_counters = || {
        Arc::new(
            (0..v3_confirm_lanes.max(1))
                .map(|_| Arc::new(AtomicU64::new(0)))
                .collect::<Vec<_>>(),
        )
    };
    let v3_confirm_age_hist_per_lane = Arc::new(
        (0..v3_confirm_lanes.max(1))
            .map(|_| Arc::new(LatencyHistogram::new()))
            .collect::<Vec<_>>(),
    );
    let v3_durable_wal_fsync_hist_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(LatencyHistogram::new()))
            .collect::<Vec<_>>(),
    );
    let v3_durable_worker_loop_hist_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(LatencyHistogram::new()))
            .collect::<Vec<_>>(),
    );

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
        v3_durable_audit_logs: Arc::new(v3_durable_audit_logs),
        audit_read_path: Arc::new(audit_read_path),
        v3_confirm_rebuild_paths: Arc::new(v3_confirm_rebuild_paths),
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
        v3_durable_ingress: Arc::clone(&v3_durable_ingress),
        v3_durable_accepted_total: Arc::new(AtomicU64::new(0)),
        v3_durable_rejected_total: Arc::new(AtomicU64::new(0)),
        v3_live_ack_hist: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_confirm_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_append_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_fsync_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_fsync_hist_per_lane,
        v3_durable_worker_loop_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_worker_loop_hist_per_lane,
        v3_durable_worker_batch_min,
        v3_durable_worker_batch_max,
        v3_durable_worker_batch_wait_min_us,
        v3_durable_worker_batch_wait_us,
        v3_durable_worker_batch_adaptive,
        v3_durable_worker_batch_adaptive_low_util_pct,
        v3_durable_worker_batch_adaptive_high_util_pct,
        v3_durable_depth_last: Arc::new(AtomicU64::new(0)),
        v3_durable_depth_last_per_lane: new_lane_u64_counters(),
        v3_durable_backlog_growth_per_sec: Arc::new(AtomicI64::new(0)),
        v3_durable_backlog_growth_per_sec_per_lane: new_lane_i64_gauges(),
        v3_durable_soft_reject_pct,
        v3_durable_hard_reject_pct,
        v3_durable_backlog_soft_reject_per_sec,
        v3_durable_backlog_hard_reject_per_sec,
        v3_durable_backlog_signal_min_queue_pct,
        v3_durable_admission_controller_enabled,
        v3_durable_admission_sustain_ticks,
        v3_durable_admission_recover_ticks,
        v3_durable_admission_soft_fsync_p99_us,
        v3_durable_admission_hard_fsync_p99_us,
        v3_durable_admission_fsync_presignal_pct,
        v3_durable_admission_level: Arc::new(AtomicU64::new(0)),
        v3_durable_admission_level_per_lane: new_lane_u64_counters(),
        v3_durable_admission_soft_trip_total: Arc::new(AtomicU64::new(0)),
        v3_durable_admission_soft_trip_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_hard_trip_total: Arc::new(AtomicU64::new(0)),
        v3_durable_admission_hard_trip_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_signal_queue_soft_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_signal_queue_hard_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_signal_backlog_soft_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_signal_backlog_hard_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_signal_fsync_soft_total_per_lane: new_lane_u64_counters(),
        v3_durable_admission_signal_fsync_hard_total_per_lane: new_lane_u64_counters(),
        v3_durable_backpressure_soft_total: Arc::new(AtomicU64::new(0)),
        v3_durable_backpressure_soft_total_per_lane: new_lane_u64_counters(),
        v3_durable_backpressure_hard_total: Arc::new(AtomicU64::new(0)),
        v3_durable_backpressure_hard_total_per_lane: new_lane_u64_counters(),
        v3_durable_write_error_total: Arc::new(AtomicU64::new(0)),
        v3_soft_reject_pct,
        v3_hard_reject_pct,
        v3_kill_reject_pct,
        v3_confirm_store: Arc::new(V3ConfirmStore::new(
            v3_confirm_lanes,
            v3_loss_gap_timeout_ms,
            v3_confirm_ttl_ms,
        )),
        v3_confirm_oldest_inflight_us: Arc::new(AtomicU64::new(0)),
        v3_confirm_oldest_inflight_us_per_lane: new_confirm_lane_u64_counters(),
        v3_confirm_age_hist_per_lane,
        v3_loss_gap_timeout_ms,
        v3_loss_scan_interval_ms,
        v3_loss_scan_batch,
        v3_confirm_gc_batch,
        v3_confirm_timeout_scan_cost_last: Arc::new(AtomicU64::new(0)),
        v3_confirm_timeout_scan_cost_total: Arc::new(AtomicU64::new(0)),
        v3_confirm_gc_removed_total: Arc::new(AtomicU64::new(0)),
        v3_confirm_rebuild_restored_total: Arc::new(AtomicU64::new(0)),
        v3_confirm_rebuild_elapsed_ms: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_soft_reject_age_us,
        v3_durable_confirm_hard_reject_age_us,
        v3_durable_confirm_age_soft_reject_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_total: Arc::new(AtomicU64::new(0)),
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

    if v3_confirm_rebuild_on_start {
        let rebuild_t0 = Instant::now();
        let restored = rebuild_v3_confirm_store_from_wal(&state, v3_confirm_rebuild_max_lines);
        let elapsed_ms = rebuild_t0.elapsed().as_millis() as u64;
        state
            .v3_confirm_rebuild_restored_total
            .store(restored, Ordering::Relaxed);
        state
            .v3_confirm_rebuild_elapsed_ms
            .store(elapsed_ms, Ordering::Relaxed);
        info!(
            restored = restored,
            elapsed_ms = elapsed_ms,
            max_lines = v3_confirm_rebuild_max_lines,
            "v3 confirm store rebuilt from WAL"
        );
    }

    for (shard_id, v3_rx) in v3_rxs.into_iter().enumerate() {
        let writer_state = state.clone();
        tokio::spawn(async move {
            run_v3_single_writer(shard_id, v3_rx, writer_state).await;
        });
    }
    for (lane_id, v3_durable_rx) in v3_durable_rxs.into_iter().enumerate() {
        let durable_state = state.clone();
        let durable_batch_max = durable_state.v3_durable_worker_batch_max;
        let durable_batch_wait_us = durable_state.v3_durable_worker_batch_wait_us;
        let durable_batch_adaptive_cfg = v3_durable_worker_batch_adaptive_cfg;
        tokio::spawn(async move {
            run_v3_durable_worker(
                lane_id,
                v3_durable_rx,
                durable_state,
                durable_batch_max,
                durable_batch_wait_us,
                durable_batch_adaptive_cfg,
            )
            .await;
        });
    }

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

    let v3_http_enable = parse_bool_env("V3_HTTP_ENABLE").unwrap_or(true);
    let v3_tcp_enable = parse_bool_env("V3_TCP_ENABLE").unwrap_or(false);
    let v3_tcp_port = std::env::var("V3_TCP_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(0);
    if v3_tcp_enable && v3_tcp_port > 0 {
        let v3_tcp_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = run_v3_tcp_server(v3_tcp_port, v3_tcp_state).await {
                tracing::error!(error = %err, "v3 tcp server exited");
            }
        });
    }

    let app_base = Router::new()
        .route("/orders", post(handle_order))
        .route("/v2/orders", post(handle_order_v2))
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
        .route("/metrics", get(handle_metrics));
    let app = if v3_http_enable {
        app_base.route("/v3/orders", post(handle_order_v3)).route(
            "/v3/orders/{session_id}/{session_seq}",
            get(handle_get_order_v3),
        )
    } else {
        app_base
    }
    .layer(CorsLayer::permissive())
    .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("HTTP server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn run_v3_tcp_server(port: u16, state: AppState) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("v3 TCP server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        let conn_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_v3_tcp_connection(socket, conn_state).await {
                tracing::warn!(peer = %peer, error = %err, "v3 tcp connection error");
            }
        });
    }
}

async fn handle_v3_tcp_connection(
    mut socket: tokio::net::TcpStream,
    state: AppState,
) -> anyhow::Result<()> {
    socket.set_nodelay(true)?;
    let mut req = [0u8; V3_TCP_REQUEST_SIZE];
    loop {
        match socket.read_exact(&mut req).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let t0 = gateway_core::now_nanos();
        let resp = match decode_v3_tcp_request(&req) {
            Ok(decoded) => match authenticate_v3_tcp_token(&state, &decoded.jwt_token) {
                Ok(principal) => {
                    let (status, body) = process_order_v3_hot_path(
                        &state,
                        principal.session_id,
                        decoded.order_req,
                        t0,
                    );
                    encode_v3_tcp_response(status, &body)
                }
                Err((status, reason_code)) => encode_v3_tcp_decode_error(status, reason_code, t0),
            },
            Err(code) => encode_v3_tcp_decode_error(StatusCode::UNPROCESSABLE_ENTITY, code, t0),
        };
        socket.write_all(&resp).await?;
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct V3DurableWorkerBatchAdaptiveConfig {
    enabled: bool,
    batch_min: usize,
    batch_max: usize,
    wait_min: Duration,
    wait_max: Duration,
    low_util_pct: f64,
    high_util_pct: f64,
}

impl V3DurableWorkerBatchAdaptiveConfig {
    fn target_for_pressure(&self, pressure_pct: f64) -> (usize, Duration) {
        let batch_min = self.batch_min.max(1).min(self.batch_max.max(1));
        let batch_max = self.batch_max.max(batch_min);
        let wait_min_us = self.wait_min.as_micros() as u64;
        let wait_max_us = self.wait_max.as_micros() as u64;
        let wait_min_us = wait_min_us.max(1).min(wait_max_us.max(1));
        let wait_max_us = wait_max_us.max(wait_min_us);
        if !self.enabled || (self.high_util_pct - self.low_util_pct) <= f64::EPSILON {
            return (batch_max, Duration::from_micros(wait_max_us));
        }
        if pressure_pct <= self.low_util_pct {
            return (batch_min, Duration::from_micros(wait_min_us));
        }
        if pressure_pct >= self.high_util_pct {
            return (batch_max, Duration::from_micros(wait_max_us));
        }
        let ratio = ((pressure_pct - self.low_util_pct) / (self.high_util_pct - self.low_util_pct))
            .clamp(0.0, 1.0);
        let batch_span = batch_max.saturating_sub(batch_min);
        let wait_span_us = wait_max_us.saturating_sub(wait_min_us);
        let batch =
            batch_min + ((batch_span as f64 * ratio).round() as usize).min(batch_span.max(1));
        let wait_us =
            wait_min_us + ((wait_span_us as f64 * ratio).round() as u64).min(wait_span_us);
        (batch.max(1), Duration::from_micros(wait_us.max(1)))
    }
}

fn v3_pressure_ratio(current: i64, soft: i64, hard: i64) -> f64 {
    if hard <= soft {
        return if current >= hard { 1.0 } else { 0.0 };
    }
    let current = current.max(0);
    let soft = soft.max(0);
    let hard = hard.max(1);
    if current <= soft {
        0.0
    } else if current >= hard {
        1.0
    } else {
        (current - soft) as f64 / (hard - soft) as f64
    }
}

fn v3_fsync_pressure_ratio(current_us: u64, soft_us: u64, hard_us: u64) -> f64 {
    if hard_us <= soft_us {
        return if current_us >= hard_us { 1.0 } else { 0.0 };
    }
    if current_us <= soft_us {
        0.0
    } else if current_us >= hard_us {
        1.0
    } else {
        (current_us - soft_us) as f64 / (hard_us - soft_us) as f64
    }
}

/// /v3/orders の single-writer worker。
/// ホットキューを直列消費し、durable confirm 経路へ渡す。
async fn run_v3_single_writer(shard_id: usize, mut rx: Receiver<V3OrderTask>, state: AppState) {
    while let Some(task) = rx.recv().await {
        state.v3_ingress.on_processed_one(shard_id);
        state
            .v3_confirm_store
            .record_volatile(&task, gateway_core::now_nanos());
        match state.v3_durable_ingress.try_enqueue(task.into()) {
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
async fn run_v3_durable_worker(
    lane_id: usize,
    mut rx: Receiver<V3DurableTask>,
    state: AppState,
    batch_max: usize,
    batch_wait_us: u64,
    batch_adaptive_cfg: V3DurableWorkerBatchAdaptiveConfig,
) {
    let fallback_batch_max = batch_max.max(1);
    let fallback_batch_wait = Duration::from_micros(batch_wait_us.max(1));
    let lane_audit_log = state
        .v3_durable_audit_logs
        .get(lane_id)
        .cloned()
        .unwrap_or_else(|| Arc::clone(&state.audit_log));
    loop {
        let Some(first) = rx.recv().await else {
            break;
        };
        let lane_util_pct = state.v3_durable_ingress.lane_utilization_pct(lane_id);
        let lane_backlog_growth_per_sec = state
            .v3_durable_backlog_growth_per_sec_per_lane
            .get(lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0);
        let lane_fsync_p99_us = state
            .v3_durable_wal_fsync_hist_per_lane
            .get(lane_id)
            .map(|hist| hist.snapshot().percentile(99.0))
            .unwrap_or_else(|| state.v3_durable_wal_fsync_hist.snapshot().percentile(99.0));
        let util_ratio = (lane_util_pct / 100.0).clamp(0.0, 1.0);
        let backlog_ratio = v3_pressure_ratio(
            lane_backlog_growth_per_sec,
            state.v3_durable_backlog_soft_reject_per_sec,
            state.v3_durable_backlog_hard_reject_per_sec,
        );
        let fsync_ratio = v3_fsync_pressure_ratio(
            lane_fsync_p99_us,
            state.v3_durable_admission_soft_fsync_p99_us,
            state.v3_durable_admission_hard_fsync_p99_us,
        );
        let lane_pressure_pct = ((util_ratio * 0.60 + backlog_ratio * 0.25 + fsync_ratio * 0.15)
            * 100.0)
            .clamp(0.0, 100.0);
        let (target_batch_max, target_batch_wait) = if batch_adaptive_cfg.enabled {
            batch_adaptive_cfg.target_for_pressure(lane_pressure_pct)
        } else {
            (fallback_batch_max, fallback_batch_wait)
        };
        let mut batch = Vec::with_capacity(target_batch_max.max(1));
        batch.push(first);
        if target_batch_max > 1 {
            let deadline = tokio::time::Instant::now() + target_batch_wait;
            while batch.len() < target_batch_max {
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(task)) => batch.push(task),
                    Ok(None) | Err(_) => break,
                }
            }
        }
        let worker_loop_t0 = gateway_core::now_nanos();
        let mut pending = Vec::with_capacity(batch.len());
        for task in batch {
            state.v3_durable_ingress.on_processed_one(lane_id);
            let append_t0 = gateway_core::now_nanos();
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
            let append = lane_audit_log.append_with_durable_receipt(event, append_t0);
            let append_elapsed_us = if append.timings.enqueue_done_ns >= append_t0 {
                (append.timings.enqueue_done_ns - append_t0) / 1_000
            } else {
                gateway_core::now_nanos().saturating_sub(append_t0) / 1_000
            };
            state.v3_durable_wal_append_hist.record(append_elapsed_us);
            pending.push((
                task,
                append.timings.durable_done_ns,
                append.timings.fdatasync_ns,
                append.durable_rx,
            ));
        }

        // 先にbatch全体をWAL queueへ積み、後段でdurable receiptを回収する。
        // enqueueとwaitを分離してgroup commitを効かせる。
        for (task, durable_done_ns_hint, fdatasync_ns_hint, durable_rx) in pending {
            let (durable_done_ns, fdatasync_ns, reject_reason) = if durable_done_ns_hint > 0 {
                (durable_done_ns_hint, fdatasync_ns_hint, "")
            } else if let Some(durable_rx) = durable_rx {
                match durable_rx.await {
                    Ok(receipt) if receipt.durable_done_ns > 0 => {
                        (receipt.durable_done_ns, receipt.fdatasync_ns, "")
                    }
                    Ok(_) => (0, 0, "WAL_DURABILITY_FAILED"),
                    Err(_) => (0, 0, "WAL_DURABILITY_RECEIPT_CLOSED"),
                }
            } else {
                (0, 0, "WAL_DURABILITY_ENQUEUE_FAILED")
            };
            if fdatasync_ns > 0 {
                state.v3_durable_wal_fsync_hist.record(fdatasync_ns / 1_000);
                if let Some(hist) = state.v3_durable_wal_fsync_hist_per_lane.get(lane_id) {
                    hist.record(fdatasync_ns / 1_000);
                }
            }

            let now_ns = gateway_core::now_nanos();
            if durable_done_ns > 0 {
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
                    reject_reason,
                    now_ns,
                );
                state
                    .v3_durable_rejected_total
                    .fetch_add(1, Ordering::Relaxed);
                state
                    .v3_durable_write_error_total
                    .fetch_add(1, Ordering::Relaxed);
            }
            let elapsed_us = now_ns.saturating_sub(task.received_at_ns) / 1_000;
            state.v3_durable_confirm_hist.record(elapsed_us);
        }
        let worker_loop_elapsed_us =
            gateway_core::now_nanos().saturating_sub(worker_loop_t0) / 1_000;
        state
            .v3_durable_worker_loop_hist
            .record(worker_loop_elapsed_us);
        if let Some(hist) = state.v3_durable_worker_loop_hist_per_lane.get(lane_id) {
            hist.record(worker_loop_elapsed_us);
        }
    }
}

/// /v3 の未確定注文を監視し、timeout 時は LOSS_SUSPECT へ昇格する。
async fn run_v3_loss_monitor(state: AppState) {
    let interval_ms = state.v3_loss_scan_interval_ms.max(10);
    let mut ticker = tokio::time::interval(Duration::from_millis(interval_ms));
    let lane_count = state.v3_durable_ingress.lane_count();
    let mut soft_streak_per_lane = vec![0u64; lane_count];
    let mut hard_streak_per_lane = vec![0u64; lane_count];
    let mut clear_streak_per_lane = vec![0u64; lane_count];
    loop {
        ticker.tick().await;
        let now_ns = gateway_core::now_nanos();
        let confirm_oldest_age_us_per_lane = state
            .v3_confirm_store
            .oldest_volatile_age_us_per_lane(now_ns);
        let mut confirm_oldest_age_us_max = 0u64;
        for (lane_id, age_us) in confirm_oldest_age_us_per_lane.iter().copied().enumerate() {
            confirm_oldest_age_us_max = confirm_oldest_age_us_max.max(age_us);
            if let Some(gauge) = state.v3_confirm_oldest_inflight_us_per_lane.get(lane_id) {
                gauge.store(age_us, Ordering::Relaxed);
            }
            if let Some(hist) = state.v3_confirm_age_hist_per_lane.get(lane_id) {
                hist.record(age_us);
            }
        }
        state
            .v3_confirm_oldest_inflight_us
            .store(confirm_oldest_age_us_max, Ordering::Relaxed);
        let scan = state
            .v3_confirm_store
            .collect_timed_out(now_ns, state.v3_loss_scan_batch);
        state
            .v3_confirm_timeout_scan_cost_last
            .store(scan.scan_cost, Ordering::Relaxed);
        state
            .v3_confirm_timeout_scan_cost_total
            .fetch_add(scan.scan_cost, Ordering::Relaxed);
        for candidate in scan.candidates {
            state.register_v3_loss_suspect(
                &candidate.session_id,
                candidate.session_seq,
                candidate.shard_id,
                "DURABLE_CONFIRM_TIMEOUT",
                now_ns,
            );
        }
        let gc_removed = state
            .v3_confirm_store
            .gc_expired(now_ns, state.v3_confirm_gc_batch);
        state
            .v3_confirm_gc_removed_total
            .fetch_add(gc_removed as u64, Ordering::Relaxed);
        let durable_depth_now = state.v3_durable_ingress.total_depth();
        let durable_depth_prev = state
            .v3_durable_depth_last
            .swap(durable_depth_now, Ordering::Relaxed);
        let growth_i128 = (durable_depth_now as i128)
            .saturating_sub(durable_depth_prev as i128)
            .saturating_mul(1_000)
            .checked_div(interval_ms as i128)
            .unwrap_or(0);
        let growth = growth_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
        state
            .v3_durable_backlog_growth_per_sec
            .store(growth, Ordering::Relaxed);
        for lane_id in 0..lane_count {
            let lane_depth_now = state.v3_durable_ingress.lane_depth(lane_id);
            let lane_depth_prev = state
                .v3_durable_depth_last_per_lane
                .get(lane_id)
                .map(|counter| counter.swap(lane_depth_now, Ordering::Relaxed))
                .unwrap_or(0);
            let lane_growth_i128 = (lane_depth_now as i128)
                .saturating_sub(lane_depth_prev as i128)
                .saturating_mul(1_000)
                .checked_div(interval_ms as i128)
                .unwrap_or(0);
            let lane_growth = lane_growth_i128.clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            if let Some(gauge) = state
                .v3_durable_backlog_growth_per_sec_per_lane
                .get(lane_id)
            {
                gauge.store(lane_growth, Ordering::Relaxed);
            }
        }
        if state.v3_durable_admission_controller_enabled {
            let prev_level = state.v3_durable_admission_level.load(Ordering::Relaxed);
            let mut next_level_global = 0u64;
            for lane_id in 0..lane_count {
                let lane_queue_pct_now = state.v3_durable_ingress.lane_utilization_pct(lane_id);
                let lane_growth = state
                    .v3_durable_backlog_growth_per_sec_per_lane
                    .get(lane_id)
                    .map(|v| v.load(Ordering::Relaxed))
                    .unwrap_or(0);
                let lane_fsync_p99_us = state
                    .v3_durable_wal_fsync_hist_per_lane
                    .get(lane_id)
                    .map(|hist| hist.snapshot().percentile(99.0))
                    .unwrap_or_else(|| state.v3_durable_wal_fsync_hist.snapshot().percentile(99.0));
                let queue_pressure_soft =
                    lane_queue_pct_now >= state.v3_durable_soft_reject_pct as f64;
                let queue_pressure_hard =
                    lane_queue_pct_now >= state.v3_durable_hard_reject_pct as f64;
                let backlog_signal_enabled =
                    lane_queue_pct_now >= state.v3_durable_backlog_signal_min_queue_pct;
                let backlog_pressure_soft = backlog_signal_enabled
                    && lane_growth >= state.v3_durable_backlog_soft_reject_per_sec;
                let backlog_pressure_hard = backlog_signal_enabled
                    && lane_growth >= state.v3_durable_backlog_hard_reject_per_sec;
                let fsync_soft = lane_fsync_p99_us >= state.v3_durable_admission_soft_fsync_p99_us;
                let fsync_hard = lane_fsync_p99_us >= state.v3_durable_admission_hard_fsync_p99_us;
                let fsync_presignal_queue_pct = (state.v3_durable_soft_reject_pct as f64
                    * state.v3_durable_admission_fsync_presignal_pct)
                    .clamp(0.0, 100.0);
                let fsync_presignal_backlog = ((state.v3_durable_backlog_soft_reject_per_sec
                    as f64)
                    * state.v3_durable_admission_fsync_presignal_pct)
                    .round()
                    .max(0.0) as i64;
                let hard_signal = queue_pressure_hard
                    || backlog_pressure_hard
                    || ((queue_pressure_soft || backlog_pressure_soft) && fsync_hard);
                let soft_signal = queue_pressure_soft
                    || backlog_pressure_soft
                    || ((lane_queue_pct_now >= fsync_presignal_queue_pct
                        || (backlog_signal_enabled && lane_growth >= fsync_presignal_backlog))
                        && fsync_soft);

                if queue_pressure_soft {
                    if let Some(counter) = state
                        .v3_durable_admission_signal_queue_soft_total_per_lane
                        .get(lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if queue_pressure_hard {
                    if let Some(counter) = state
                        .v3_durable_admission_signal_queue_hard_total_per_lane
                        .get(lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if backlog_pressure_soft {
                    if let Some(counter) = state
                        .v3_durable_admission_signal_backlog_soft_total_per_lane
                        .get(lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if backlog_pressure_hard {
                    if let Some(counter) = state
                        .v3_durable_admission_signal_backlog_hard_total_per_lane
                        .get(lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if fsync_soft {
                    if let Some(counter) = state
                        .v3_durable_admission_signal_fsync_soft_total_per_lane
                        .get(lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
                if fsync_hard {
                    if let Some(counter) = state
                        .v3_durable_admission_signal_fsync_hard_total_per_lane
                        .get(lane_id)
                    {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                }

                if hard_signal {
                    hard_streak_per_lane[lane_id] = hard_streak_per_lane[lane_id].saturating_add(1);
                    soft_streak_per_lane[lane_id] = soft_streak_per_lane[lane_id].saturating_add(1);
                    clear_streak_per_lane[lane_id] = 0;
                } else if soft_signal {
                    soft_streak_per_lane[lane_id] = soft_streak_per_lane[lane_id].saturating_add(1);
                    hard_streak_per_lane[lane_id] = 0;
                    clear_streak_per_lane[lane_id] = 0;
                } else {
                    clear_streak_per_lane[lane_id] =
                        clear_streak_per_lane[lane_id].saturating_add(1);
                    soft_streak_per_lane[lane_id] = 0;
                    hard_streak_per_lane[lane_id] = 0;
                }

                let prev_lane_level = state
                    .v3_durable_admission_level_per_lane
                    .get(lane_id)
                    .map(|level| level.load(Ordering::Relaxed))
                    .unwrap_or(0);
                let mut next_lane_level = prev_lane_level;
                if hard_signal
                    && hard_streak_per_lane[lane_id] >= state.v3_durable_admission_sustain_ticks
                {
                    next_lane_level = 2;
                } else if soft_signal
                    && soft_streak_per_lane[lane_id] >= state.v3_durable_admission_sustain_ticks
                {
                    next_lane_level = 1;
                } else if !soft_signal
                    && clear_streak_per_lane[lane_id] >= state.v3_durable_admission_recover_ticks
                {
                    next_lane_level = 0;
                }
                if let Some(level) = state.v3_durable_admission_level_per_lane.get(lane_id) {
                    if next_lane_level != prev_lane_level {
                        level.store(next_lane_level, Ordering::Relaxed);
                        if next_lane_level == 1 && prev_lane_level < 1 {
                            if let Some(counter) = state
                                .v3_durable_admission_soft_trip_total_per_lane
                                .get(lane_id)
                            {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        if next_lane_level == 2 && prev_lane_level < 2 {
                            if let Some(counter) = state
                                .v3_durable_admission_hard_trip_total_per_lane
                                .get(lane_id)
                            {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
                next_level_global = next_level_global.max(next_lane_level);
            }

            if next_level_global != prev_level {
                state
                    .v3_durable_admission_level
                    .store(next_level_global, Ordering::Relaxed);
                if next_level_global == 1 && prev_level < 1 {
                    state
                        .v3_durable_admission_soft_trip_total
                        .fetch_add(1, Ordering::Relaxed);
                }
                if next_level_global == 2 && prev_level < 2 {
                    state
                        .v3_durable_admission_hard_trip_total
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::{BufReader, Cursor};

    fn test_task(
        session_id: &str,
        session_seq: u64,
        received_at_ns: u64,
        shard_id: usize,
    ) -> V3OrderTask {
        V3OrderTask {
            session_id: session_id.to_string(),
            session_seq,
            attempt_seq: session_seq,
            received_at_ns,
            shard_id,
        }
    }

    #[test]
    fn v3_durable_ingress_routes_lane_by_shard_id() {
        let mut lanes = Vec::new();
        let mut _rxs = Vec::new();
        for _ in 0..4 {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            lanes.push(V3DurableLane::new(tx, 8));
            _rxs.push(rx);
        }
        let ingress = V3DurableIngress::new(lanes);

        for shard_id in 0..8 {
            let task = V3DurableTask {
                session_id: format!("sess-{}", shard_id),
                session_seq: shard_id as u64,
                attempt_seq: shard_id as u64,
                received_at_ns: 1_000_000,
                shard_id,
            };
            ingress
                .try_enqueue(task)
                .expect("enqueue should succeed for test lane capacity");
        }

        assert_eq!(ingress.lane_depths(), vec![2, 2, 2, 2]);
    }

    #[test]
    fn v3_confirm_store_lane_hash_is_stable_and_skew_is_observable() {
        let store = V3ConfirmStore::new(4, 500, 60_000);
        let lane_a = store.lane_for_session("sess-a");
        let lane_b = store.lane_for_session("sess-a");
        assert_eq!(lane_a, lane_b);
        assert_eq!(store.lane_count_metric(), 4);

        for seq in 0..8 {
            let task = test_task("sess-a", seq, 1_000_000, 0);
            store.record_volatile(&task, 1_000_000);
        }
        assert_eq!(store.total_size(), 8);
        assert!(store.lane_skew_pct() > 0.0);
    }

    #[test]
    fn v3_confirm_store_timeout_wheel_scans_only_due_entries() {
        let store = V3ConfirmStore::new(4, 500, 60_000);
        let far_future_ns = 10_000_000_000u64;
        for seq in 0..128 {
            let task = test_task(&format!("sess-{}", seq), seq, far_future_ns, 0);
            store.record_volatile(&task, far_future_ns);
        }
        let pre_scan = store.collect_timed_out(1_000_000, 256);
        assert!(pre_scan.candidates.is_empty());
        assert_eq!(pre_scan.scan_cost, 0);

        let due_received_ns = 2_000_000u64;
        let due = test_task("sess-due", 1, due_received_ns, 2);
        store.record_volatile(&due, due_received_ns);
        let at_deadline = due_received_ns.saturating_add(store.timeout_ns);
        let scan = store.collect_timed_out(at_deadline, 16);
        assert_eq!(scan.scan_cost, 1);
        assert_eq!(scan.candidates.len(), 1);
        assert_eq!(scan.candidates[0].session_id, "sess-due");
        assert_eq!(scan.candidates[0].session_seq, 1);
        assert_eq!(scan.candidates[0].shard_id, 2);
    }

    #[test]
    fn v3_confirm_store_ttl_gc_removes_expired_records() {
        let store = V3ConfirmStore::new(1, 500, 2);
        let received_ns = 10_000_000u64;
        let task = test_task("sess-gc", 7, received_ns, 0);
        store.record_volatile(&task, received_ns);
        let durable_ns = received_ns.saturating_add(1_000_000);
        store.mark_durable_accepted("sess-gc", 7, durable_ns);

        let before_ttl = durable_ns.saturating_add(store.ttl_ns).saturating_sub(1);
        assert_eq!(store.gc_expired(before_ttl, 64), 0);
        assert!(store.snapshot("sess-gc", 7).is_some());

        let at_ttl = durable_ns.saturating_add(store.ttl_ns);
        assert_eq!(store.gc_expired(at_ttl, 64), 1);
        assert!(store.snapshot("sess-gc", 7).is_none());
    }

    #[test]
    fn v3_confirm_store_rebuilds_from_wal_reader() {
        let store = V3ConfirmStore::new(2, 500, 60_000);
        let events = vec![
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 1,
                account_id: "acc-a".to_string(),
                order_id: Some("v3/sess-a/11".to_string()),
                data: json!({}),
            })
            .expect("serialize accepted event"),
            "not-json-line".to_string(),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableRejected".to_string(),
                at: 2,
                account_id: "acc-b".to_string(),
                order_id: Some("v3/sess-b/22".to_string()),
                data: json!({ "reason": "RISK_REJECTED" }),
            })
            .expect("serialize rejected event"),
            serde_json::to_string(&AuditEvent {
                event_type: "OrderAccepted".to_string(),
                at: 3,
                account_id: "acc-c".to_string(),
                order_id: Some("ord-legacy".to_string()),
                data: json!({}),
            })
            .expect("serialize non-v3 event"),
        ]
        .join("\n");
        let reader = BufReader::new(Cursor::new(events.into_bytes()));
        let restored = rebuild_v3_confirm_store_from_reader(&store, reader, 1024);
        assert_eq!(restored, 2);

        let accepted = store
            .snapshot("sess-a", 11)
            .expect("accepted snapshot must exist");
        assert_eq!(accepted.status, V3ConfirmStatus::DurableAccepted);

        let rejected = store
            .snapshot("sess-b", 22)
            .expect("rejected snapshot must exist");
        assert_eq!(rejected.status, V3ConfirmStatus::DurableRejected);
        assert_eq!(rejected.reason.as_deref(), Some("RISK_REJECTED"));
    }

    #[test]
    fn v3_confirm_store_rebuild_keeps_latest_event_without_duplicates() {
        let store = V3ConfirmStore::new(2, 500, 60_000);
        let events = vec![
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 1,
                account_id: "acc-a".to_string(),
                order_id: Some("v3/sess-a/1".to_string()),
                data: json!({}),
            })
            .expect("serialize accepted event a1"),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableRejected".to_string(),
                at: 2,
                account_id: "acc-a".to_string(),
                order_id: Some("v3/sess-a/1".to_string()),
                data: json!({ "reason": "LATE_REJECT" }),
            })
            .expect("serialize rejected event a1"),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 3,
                account_id: "acc-b".to_string(),
                order_id: Some("v3/sess-b/2".to_string()),
                data: json!({}),
            })
            .expect("serialize accepted event b2"),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 4,
                account_id: "acc-c".to_string(),
                order_id: Some("v3/sess-c/3".to_string()),
                data: json!({}),
            })
            .expect("serialize accepted event c3"),
        ]
        .join("\n");
        let reader = BufReader::new(Cursor::new(events.into_bytes()));
        let restored = rebuild_v3_confirm_store_from_reader(&store, reader, 3);
        assert_eq!(restored, 3);
        assert_eq!(store.total_size(), 3);

        let sess_a = store
            .snapshot("sess-a", 1)
            .expect("sess-a snapshot must exist");
        assert_eq!(sess_a.status, V3ConfirmStatus::DurableRejected);
        assert_eq!(sess_a.reason.as_deref(), Some("LATE_REJECT"));
        let sess_b = store
            .snapshot("sess-b", 2)
            .expect("sess-b snapshot must exist");
        assert_eq!(sess_b.status, V3ConfirmStatus::DurableAccepted);
        let sess_c = store
            .snapshot("sess-c", 3)
            .expect("sess-c snapshot must exist");
        assert_eq!(sess_c.status, V3ConfirmStatus::DurableAccepted);
    }

    #[test]
    fn parse_v3_order_id_accepts_only_expected_format() {
        assert_eq!(
            parse_v3_order_id("v3/sess-1/42"),
            Some(("sess-1".to_string(), 42))
        );
        assert_eq!(parse_v3_order_id("v3/sess-1/not-a-number"), None);
        assert_eq!(parse_v3_order_id("sess-1/42"), None);
        assert_eq!(parse_v3_order_id("v3//42"), None);
    }
}
