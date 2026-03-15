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
//!   - /orders/{id}/replace: 注文数量/価格の置換要求を非同期で流す。
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
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver,
    error::{TryRecvError, TrySendError},
};
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::audit::AuditLog;
use crate::audit::{AuditAppendWithReceipt, AuditDurableNotification, AuditEvent};
use crate::auth::JwtAuth;
use crate::backpressure::BackpressureConfig;
use crate::bus::BusPublisher;
use crate::engine::FastPathEngine;
use crate::inflight::InflightControllerHandle;
use crate::rate_limit::AccountRateLimiter;
use crate::sse::SseHub;
use crate::store::{OrderIdMap, OrderStore, ShardedOrderStore};
use gateway_core::{LatencyHistogram, RdtscpClock, RdtscpStamp};
use std::path::{Path, PathBuf};

use audit::{handle_account_events, handle_audit_anchor, handle_audit_verify, handle_order_events};
use metrics::{handle_health, handle_metrics};
use orders::{
    V3_TCP_REQUEST_SIZE, authenticate_v3_tcp_token, decode_v3_tcp_request,
    encode_v3_tcp_decode_error, encode_v3_tcp_response, handle_amend_order, handle_cancel_order,
    handle_get_order, handle_get_order_by_client_id, handle_get_order_v2, handle_get_order_v3,
    handle_order, handle_order_v2, handle_order_v3, handle_replace_order,
    process_order_v3_hot_path_tcp,
};
use sse::{handle_account_stream, handle_order_stream};

/// /v3/orders の single-writer に流す最小タスク。
#[derive(Debug)]
pub(super) struct V3OrderTask {
    pub(super) session_id: Arc<str>,
    pub(super) account_id: Arc<str>,
    pub(super) position_symbol_key: [u8; 8],
    pub(super) position_delta_qty: i64,
    pub(super) session_seq: u64,
    pub(super) attempt_seq: u64,
    pub(super) received_at_ns: u64,
    pub(super) shard_id: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct V3AccountSymbolKey {
    pub(super) account_id: Arc<str>,
    pub(super) symbol_key: [u8; 8],
}

impl V3AccountSymbolKey {
    #[inline]
    pub(super) fn new(account_id: Arc<str>, symbol_key: [u8; 8]) -> Self {
        Self {
            account_id,
            symbol_key,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct V3DurableTask {
    pub(super) session_id: Arc<str>,
    pub(super) account_id: Arc<str>,
    pub(super) position_symbol_key: [u8; 8],
    pub(super) position_delta_qty: i64,
    pub(super) session_seq: u64,
    pub(super) attempt_seq: u64,
    pub(super) received_at_ns: u64,
    pub(super) shard_id: usize,
}

impl From<V3OrderTask> for V3DurableTask {
    fn from(task: V3OrderTask) -> Self {
        Self {
            session_id: task.session_id,
            account_id: task.account_id,
            position_symbol_key: task.position_symbol_key,
            position_delta_qty: task.position_delta_qty,
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

    fn try_enqueue(
        &self,
        shard_id: usize,
        task: V3DurableTask,
    ) -> Result<(), TrySendError<V3DurableTask>> {
        // durable lane は ingress shard と同じ分割キーで固定し、順序と局所性を揃える。
        let lane_id = self.lane_for_shard(shard_id);
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

    #[inline]
    fn clamp_lane(&self, lane_id: usize) -> usize {
        lane_id.min(self.lane_count().saturating_sub(1))
    }

    fn lane_for_session(&self, session_id: &str) -> usize {
        index_for_key(session_id, self.lane_count())
    }

    fn schedule_timeout_owned(
        &self,
        lane: usize,
        session_id: String,
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
            session_id,
            session_seq,
        });
    }

    fn schedule_timeout(
        &self,
        lane: usize,
        session_id: &str,
        session_seq: u64,
        received_at_ns: u64,
    ) {
        self.schedule_timeout_owned(lane, session_id.to_string(), session_seq, received_at_ns);
    }

    fn schedule_gc_owned(
        &self,
        lane: usize,
        session_id: String,
        session_seq: u64,
        updated_at_ns: u64,
    ) {
        let deadline_ns = updated_at_ns.saturating_add(self.ttl_ns);
        let mut wheel = match self.gc_wheels[lane].lock() {
            Ok(v) => v,
            Err(poisoned) => poisoned.into_inner(),
        };
        wheel.push(V3DeadlineEntry {
            deadline_ns,
            session_id,
            session_seq,
        });
    }

    fn schedule_gc(&self, lane: usize, session_id: &str, session_seq: u64, updated_at_ns: u64) {
        self.schedule_gc_owned(lane, session_id.to_string(), session_seq, updated_at_ns);
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
        let lane = self.lane_for_session(task.session_id.as_ref());
        self.record_volatile_in_lane(lane, task, now_ns);
    }

    pub(super) fn record_volatile_in_lane(&self, lane_id: usize, task: &V3OrderTask, now_ns: u64) {
        let lane = self.clamp_lane(lane_id);
        self.records[lane].insert(
            (task.session_id.to_string(), task.session_seq),
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
            task.session_id.as_ref(),
            task.session_seq,
            task.received_at_ns,
        );
        self.schedule_gc(lane, task.session_id.as_ref(), task.session_seq, now_ns);
    }

    pub(super) fn mark_durable_accepted(&self, session_id: &str, session_seq: u64, now_ns: u64) {
        let lane = self.lane_for_session(session_id);
        self.mark_durable_accepted_in_lane(lane, session_id, session_seq, now_ns);
    }

    pub(super) fn mark_durable_accepted_in_lane(
        &self,
        lane_id: usize,
        session_id: &str,
        session_seq: u64,
        now_ns: u64,
    ) {
        self.mark_durable_accepted_in_lane_owned(
            lane_id,
            session_id.to_string(),
            session_seq,
            now_ns,
        );
    }

    pub(super) fn mark_durable_accepted_in_lane_owned(
        &self,
        lane_id: usize,
        session_id: String,
        session_seq: u64,
        now_ns: u64,
    ) {
        let lane = self.clamp_lane(lane_id);
        let key = (session_id.clone(), session_seq);
        if let Some(mut entry) = self.records[lane].get_mut(&key) {
            entry.status = V3ConfirmStatus::DurableAccepted;
            entry.reason = None;
            entry.updated_at_ns = now_ns;
            self.schedule_gc_owned(lane, session_id, session_seq, now_ns);
            return;
        }
        self.records[lane].insert(
            key,
            V3ConfirmRecord {
                status: V3ConfirmStatus::DurableAccepted,
                reason: None,
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id: lane,
            },
        );
        self.schedule_gc_owned(lane, session_id, session_seq, now_ns);
    }

    pub(super) fn mark_durable_rejected(
        &self,
        session_id: &str,
        session_seq: u64,
        reason: &str,
        now_ns: u64,
    ) {
        let lane = self.lane_for_session(session_id);
        self.mark_durable_rejected_in_lane(lane, session_id, session_seq, reason, now_ns);
    }

    pub(super) fn mark_durable_rejected_in_lane(
        &self,
        lane_id: usize,
        session_id: &str,
        session_seq: u64,
        reason: &str,
        now_ns: u64,
    ) {
        self.mark_durable_rejected_in_lane_owned(
            lane_id,
            session_id.to_string(),
            session_seq,
            reason.to_string(),
            now_ns,
        );
    }

    pub(super) fn mark_durable_rejected_in_lane_owned(
        &self,
        lane_id: usize,
        session_id: String,
        session_seq: u64,
        reason: String,
        now_ns: u64,
    ) {
        let lane = self.clamp_lane(lane_id);
        let key = (session_id.clone(), session_seq);
        if let Some(mut entry) = self.records[lane].get_mut(&key) {
            entry.status = V3ConfirmStatus::DurableRejected;
            entry.reason = Some(reason.clone());
            entry.updated_at_ns = now_ns;
            self.schedule_gc_owned(lane, session_id, session_seq, now_ns);
            return;
        }
        self.records[lane].insert(
            key,
            V3ConfirmRecord {
                status: V3ConfirmStatus::DurableRejected,
                reason: Some(reason),
                attempt_seq: session_seq,
                received_at_ns: now_ns,
                updated_at_ns: now_ns,
                shard_id: lane,
            },
        );
        self.schedule_gc_owned(lane, session_id, session_seq, now_ns);
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

    pub(super) fn seed_next_seq_floor(&self, session_id: &str, next_seq: u64) -> bool {
        let target = next_seq.max(1);
        use dashmap::mapref::entry::Entry;
        match self.session_seq.entry(session_id.to_string()) {
            Entry::Occupied(counter) => counter.get().fetch_max(target, Ordering::Relaxed) < target,
            Entry::Vacant(slot) => {
                slot.insert(Arc::new(AtomicU64::new(target)));
                true
            }
        }
    }

    pub(super) fn seed_session_shard(&self, session_id: &str, shard_id: usize) -> bool {
        if self.shards.is_empty() {
            return false;
        }
        let shard_idx = shard_id.min(self.shards.len().saturating_sub(1));
        use dashmap::mapref::entry::Entry;
        match self.session_shard.entry(session_id.to_string()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(slot) => {
                slot.insert(shard_idx);
                if let Some(counter) = self.shard_session_counts.get(shard_idx) {
                    counter.fetch_add(1, Ordering::Relaxed);
                }
                true
            }
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
    pub(super) v3_accepted_total_per_shard: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_rejected_soft_total: Arc<AtomicU64>,
    pub(super) v3_rejected_soft_total_per_shard: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_rejected_hard_total: Arc<AtomicU64>,
    pub(super) v3_rejected_hard_total_per_shard: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_rejected_killed_total: Arc<AtomicU64>,
    pub(super) v3_rejected_killed_total_per_shard: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_kill_recovered_total: Arc<AtomicU64>,
    pub(super) v3_loss_suspect_total: Arc<AtomicU64>,
    pub(super) v3_session_killed_total: Arc<AtomicU64>,
    pub(super) v3_shard_killed_total: Arc<AtomicU64>,
    pub(super) v3_global_killed_total: Arc<AtomicU64>,
    pub(super) v3_durable_ingress: Arc<V3DurableIngress>,
    pub(super) v3_durable_accepted_total: Arc<AtomicU64>,
    pub(super) v3_durable_accepted_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_rejected_total: Arc<AtomicU64>,
    pub(super) v3_durable_rejected_total_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_live_ack_hist: Arc<LatencyHistogram>,
    pub(super) v3_live_ack_hist_ns: Arc<LatencyHistogram>,
    pub(super) v3_live_ack_accepted_hist: Arc<LatencyHistogram>,
    pub(super) v3_live_ack_accepted_hist_ns: Arc<LatencyHistogram>,
    pub(super) v3_live_ack_accepted_tsc_hist_ns: Arc<LatencyHistogram>,
    pub(super) v3_tsc_clock: Option<Arc<RdtscpClock>>,
    pub(super) v3_tsc_runtime_enabled: Arc<AtomicBool>,
    pub(super) v3_tsc_invariant: bool,
    pub(super) v3_tsc_hz: u64,
    pub(super) v3_tsc_mismatch_threshold_pct: u64,
    pub(super) v3_tsc_fallback_total: Arc<AtomicU64>,
    pub(super) v3_tsc_cross_core_total: Arc<AtomicU64>,
    pub(super) v3_tsc_mismatch_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_wal_append_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_wal_fsync_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_wal_fsync_hist_per_lane: Arc<Vec<Arc<LatencyHistogram>>>,
    pub(super) v3_durable_fsync_p99_cached_us: Arc<AtomicU64>,
    pub(super) v3_durable_fsync_p99_cached_us_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_fsync_ewma_alpha_pct: u64,
    pub(super) v3_durable_worker_loop_hist: Arc<LatencyHistogram>,
    pub(super) v3_durable_worker_loop_hist_per_lane: Arc<Vec<Arc<LatencyHistogram>>>,
    pub(super) v3_durable_worker_batch_min: usize,
    pub(super) v3_durable_worker_batch_max: usize,
    pub(super) v3_durable_worker_batch_wait_min_us: u64,
    pub(super) v3_durable_worker_batch_wait_us: u64,
    pub(super) v3_durable_worker_receipt_timeout_us: u64,
    pub(super) v3_durable_replica_enabled: bool,
    pub(super) v3_durable_replica_required: bool,
    pub(super) v3_durable_replica_receipt_timeout_us: u64,
    pub(super) v3_durable_worker_max_inflight_receipts: usize,
    pub(super) v3_durable_worker_max_inflight_receipts_global: usize,
    pub(super) v3_durable_worker_inflight_soft_cap_pct: u64,
    pub(super) v3_durable_worker_inflight_hard_cap_pct: u64,
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
    pub(super) v3_durable_ack_path_guard_enabled: bool,
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
    pub(super) v3_durable_replica_append_total: Arc<AtomicU64>,
    pub(super) v3_durable_replica_write_error_total: Arc<AtomicU64>,
    pub(super) v3_durable_replica_receipt_timeout_total: Arc<AtomicU64>,
    pub(super) v3_durable_receipt_timeout_total: Arc<AtomicU64>,
    pub(super) v3_durable_receipt_inflight_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_receipt_inflight_max_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_receipt_inflight: Arc<AtomicU64>,
    pub(super) v3_durable_receipt_inflight_max: Arc<AtomicU64>,
    pub(super) v3_durable_pressure_pct_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_dynamic_cap_pct_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_soft_reject_pct: u64,
    pub(super) v3_hard_reject_pct: u64,
    pub(super) v3_kill_reject_pct: u64,
    pub(super) v3_thread_affinity_apply_success_total: Arc<AtomicU64>,
    pub(super) v3_thread_affinity_apply_failure_total: Arc<AtomicU64>,
    pub(super) v3_shard_affinity_cpu: Arc<Vec<i64>>,
    pub(super) v3_durable_affinity_cpu: Arc<Vec<i64>>,
    pub(super) v3_tcp_server_affinity_cpu: i64,
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
    pub(super) v3_replay_position_applied_total: Arc<AtomicU64>,
    pub(super) v3_replay_session_seq_seeded_total: Arc<AtomicU64>,
    pub(super) v3_replay_session_shard_seeded_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_soft_reject_age_us: u64,
    pub(super) v3_durable_confirm_hard_reject_age_us: u64,
    pub(super) v3_durable_confirm_guard_soft_slack_pct: u64,
    pub(super) v3_durable_confirm_guard_hard_slack_pct: u64,
    pub(super) v3_durable_confirm_guard_soft_requires_admission: bool,
    pub(super) v3_durable_confirm_guard_hard_requires_admission: bool,
    pub(super) v3_durable_confirm_guard_secondary_required: bool,
    pub(super) v3_durable_confirm_guard_min_queue_pct: f64,
    pub(super) v3_durable_confirm_guard_min_inflight_pct: u64,
    pub(super) v3_durable_confirm_guard_min_backlog_per_sec: i64,
    pub(super) v3_durable_confirm_guard_soft_sustain_ticks: u64,
    pub(super) v3_durable_confirm_guard_hard_sustain_ticks: u64,
    pub(super) v3_durable_confirm_guard_recover_ticks: u64,
    pub(super) v3_durable_confirm_guard_recover_hysteresis_pct: u64,
    pub(super) v3_durable_confirm_guard_autotune_enabled: bool,
    pub(super) v3_durable_confirm_guard_autotune_low_pressure_pct: u64,
    pub(super) v3_durable_confirm_guard_autotune_high_pressure_pct: u64,
    pub(super) v3_durable_confirm_guard_soft_sustain_ticks_effective_per_lane:
        Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_guard_hard_sustain_ticks_effective_per_lane:
        Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_guard_recover_ticks_effective_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_guard_soft_armed_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_guard_hard_armed_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_age_soft_reject_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_hard_reject_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_soft_reject_skipped_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_hard_reject_skipped_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_soft_reject_skipped_unarmed_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_hard_reject_skipped_unarmed_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_soft_reject_skipped_low_load_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_hard_reject_skipped_low_load_total: Arc<AtomicU64>,
    pub(super) v3_durable_confirm_age_autotune_enabled: bool,
    pub(super) v3_durable_confirm_age_autotune_alpha_pct: u64,
    pub(super) v3_durable_confirm_age_fsync_linked: bool,
    pub(super) v3_durable_confirm_age_fsync_soft_ref_us: u64,
    pub(super) v3_durable_confirm_age_fsync_hard_ref_us: u64,
    pub(super) v3_durable_confirm_age_fsync_max_relax_pct: u64,
    pub(super) v3_durable_confirm_soft_reject_age_min_us: u64,
    pub(super) v3_durable_confirm_soft_reject_age_max_us: u64,
    pub(super) v3_durable_confirm_hard_reject_age_min_us: u64,
    pub(super) v3_durable_confirm_hard_reject_age_max_us: u64,
    pub(super) v3_durable_confirm_soft_reject_age_effective_us_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_hard_reject_age_effective_us_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_durable_confirm_hourly_pressure_ewma_per_lane: Arc<Vec<Arc<AtomicU64>>>,
    pub(super) v3_risk_profile: V3RiskProfile,
    pub(super) v3_risk_margin_mode: V3RiskMarginMode,
    pub(super) v3_risk_loops: u32,
    pub(super) v3_risk_strict_symbols: bool,
    pub(super) v3_risk_max_order_qty: u64,
    pub(super) v3_risk_max_notional: u64,
    pub(super) v3_risk_daily_notional_limit: u64,
    pub(super) v3_risk_max_abs_position_qty: u64,
    pub(super) v3_symbol_limits: Arc<HashMap<[u8; 8], SymbolLimits>>,
    pub(super) v3_session_id_intern: Arc<dashmap::DashMap<String, Arc<str>>>,
    pub(super) v3_account_id_intern: Arc<dashmap::DashMap<String, Arc<str>>>,
    pub(super) v3_account_daily_notional:
        Arc<dashmap::DashMap<String, Arc<gateway_core::AccountPosition>>>,
    pub(super) v3_account_symbol_position: Arc<dashmap::DashMap<V3AccountSymbolKey, i64>>,
    pub(super) v3_hotpath_histogram_sample_rate: u64,
    pub(super) v3_hotpath_sample_cursor: Arc<AtomicU64>,
    pub(super) v3_stage_parse_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_position_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_margin_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_risk_limits_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_enqueue_hist: Arc<LatencyHistogram>,
    pub(super) v3_stage_serialize_hist: Arc<LatencyHistogram>,
}

impl AppState {
    #[inline]
    fn load_shard_counter_total(counters: &[Arc<AtomicU64>], fallback: &AtomicU64) -> u64 {
        if counters.is_empty() {
            fallback.load(Ordering::Relaxed)
        } else {
            counters
                .iter()
                .map(|value| value.load(Ordering::Relaxed))
                .sum()
        }
    }

    #[inline]
    fn increment_shard_counter(counters: &[Arc<AtomicU64>], fallback: &AtomicU64, shard_id: usize) {
        if counters.is_empty() {
            fallback.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let slot = shard_id % counters.len();
        if let Some(counter) = counters.get(slot) {
            counter.fetch_add(1, Ordering::Relaxed);
        } else {
            fallback.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    pub(super) fn increment_v3_accepted_total(&self, shard_id: usize) {
        Self::increment_shard_counter(
            &self.v3_accepted_total_per_shard,
            self.v3_accepted_total.as_ref(),
            shard_id,
        );
    }

    #[inline]
    pub(super) fn increment_v3_rejected_soft_total(&self, shard_id: usize) {
        Self::increment_shard_counter(
            &self.v3_rejected_soft_total_per_shard,
            self.v3_rejected_soft_total.as_ref(),
            shard_id,
        );
    }

    #[inline]
    pub(super) fn increment_v3_rejected_hard_total(&self, shard_id: usize) {
        Self::increment_shard_counter(
            &self.v3_rejected_hard_total_per_shard,
            self.v3_rejected_hard_total.as_ref(),
            shard_id,
        );
    }

    #[inline]
    pub(super) fn increment_v3_rejected_killed_total(&self, shard_id: usize) {
        Self::increment_shard_counter(
            &self.v3_rejected_killed_total_per_shard,
            self.v3_rejected_killed_total.as_ref(),
            shard_id,
        );
    }

    #[inline]
    pub(super) fn v3_accepted_total_current(&self) -> u64 {
        Self::load_shard_counter_total(
            &self.v3_accepted_total_per_shard,
            self.v3_accepted_total.as_ref(),
        )
    }

    #[inline]
    pub(super) fn v3_rejected_soft_total_current(&self) -> u64 {
        Self::load_shard_counter_total(
            &self.v3_rejected_soft_total_per_shard,
            self.v3_rejected_soft_total.as_ref(),
        )
    }

    #[inline]
    pub(super) fn v3_rejected_hard_total_current(&self) -> u64 {
        Self::load_shard_counter_total(
            &self.v3_rejected_hard_total_per_shard,
            self.v3_rejected_hard_total.as_ref(),
        )
    }

    #[inline]
    pub(super) fn v3_rejected_killed_total_current(&self) -> u64 {
        Self::load_shard_counter_total(
            &self.v3_rejected_killed_total_per_shard,
            self.v3_rejected_killed_total.as_ref(),
        )
    }

    #[inline]
    pub(super) fn increment_v3_durable_accepted_total(&self, lane_id: usize) {
        Self::increment_shard_counter(
            &self.v3_durable_accepted_total_per_lane,
            self.v3_durable_accepted_total.as_ref(),
            lane_id,
        );
    }

    #[inline]
    pub(super) fn increment_v3_durable_rejected_total(&self, lane_id: usize) {
        Self::increment_shard_counter(
            &self.v3_durable_rejected_total_per_lane,
            self.v3_durable_rejected_total.as_ref(),
            lane_id,
        );
    }

    #[inline]
    pub(super) fn v3_durable_accepted_total_current(&self) -> u64 {
        Self::load_shard_counter_total(
            &self.v3_durable_accepted_total_per_lane,
            self.v3_durable_accepted_total.as_ref(),
        )
    }

    #[inline]
    pub(super) fn v3_durable_rejected_total_current(&self) -> u64 {
        Self::load_shard_counter_total(
            &self.v3_durable_rejected_total_per_lane,
            self.v3_durable_rejected_total.as_ref(),
        )
    }

    #[inline]
    pub(super) fn intern_v3_session_id(&self, session_id: &str) -> Arc<str> {
        if let Some(found) = self.v3_session_id_intern.get(session_id) {
            return Arc::clone(found.value());
        }
        let interned = Arc::<str>::from(session_id);
        match self.v3_session_id_intern.entry(session_id.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(found) => Arc::clone(found.get()),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(Arc::clone(&interned));
                interned
            }
        }
    }

    #[inline]
    pub(super) fn intern_v3_account_id(&self, account_id: &str) -> Arc<str> {
        if let Some(found) = self.v3_account_id_intern.get(account_id) {
            return Arc::clone(found.value());
        }
        let interned = Arc::<str>::from(account_id);
        match self.v3_account_id_intern.entry(account_id.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(found) => Arc::clone(found.get()),
            dashmap::mapref::entry::Entry::Vacant(slot) => {
                slot.insert(Arc::clone(&interned));
                interned
            }
        }
    }

    #[inline]
    pub(super) fn v3_position_qty(&self, account_id: &Arc<str>, symbol_key: [u8; 8]) -> i64 {
        let key = V3AccountSymbolKey::new(Arc::clone(account_id), symbol_key);
        self.v3_account_symbol_position
            .get(&key)
            .map(|value| *value)
            .unwrap_or(0)
    }

    #[inline]
    pub(super) fn apply_v3_position_delta(
        &self,
        account_id: Arc<str>,
        symbol_key: [u8; 8],
        delta_qty: i64,
    ) {
        let key = V3AccountSymbolKey::new(account_id, symbol_key);
        self.v3_account_symbol_position
            .entry(key)
            .and_modify(|value| {
                *value = value.saturating_add(delta_qty);
            })
            .or_insert(delta_qty);
    }

    #[inline]
    pub(super) fn v3_hotpath_sampled(&self) -> bool {
        let rate = self.v3_hotpath_histogram_sample_rate.max(1);
        if rate <= 1 {
            return true;
        }
        let seq = self.v3_hotpath_sample_cursor.fetch_add(1, Ordering::Relaxed);
        seq % rate == 0
    }

    #[inline]
    pub(super) fn capture_v3_tsc_stamp(&self) -> Option<RdtscpStamp> {
        if !self.v3_tsc_runtime_enabled.load(Ordering::Relaxed) {
            return None;
        }
        self.v3_tsc_clock.as_ref().and_then(|clock| clock.now())
    }

    #[inline]
    pub(super) fn record_v3_tsc_accepted(
        &self,
        start: RdtscpStamp,
        end: RdtscpStamp,
        fallback_elapsed_ns: u64,
    ) {
        if !self.v3_tsc_runtime_enabled.load(Ordering::Relaxed) {
            return;
        }
        let Some(clock) = self.v3_tsc_clock.as_ref() else {
            self.v3_tsc_fallback_total.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if !clock.same_core(start, end) {
            self.v3_tsc_cross_core_total.fetch_add(1, Ordering::Relaxed);
            self.v3_tsc_fallback_total.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Some(tsc_elapsed_ns) = clock.elapsed_ns(start, end) else {
            self.v3_tsc_fallback_total.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if fallback_elapsed_ns > 0 {
            let diff_ns = tsc_elapsed_ns.abs_diff(fallback_elapsed_ns);
            let diff_pct = ((diff_ns as f64) * 100.0) / (fallback_elapsed_ns as f64);
            if diff_pct > self.v3_tsc_mismatch_threshold_pct as f64 {
                self.v3_tsc_mismatch_total.fetch_add(1, Ordering::Relaxed);
                self.v3_tsc_fallback_total.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }
        self.v3_live_ack_accepted_tsc_hist_ns.record(tsc_elapsed_ns);
    }

    #[inline]
    pub(super) fn v3_durable_confirm_reject_ages_for_lane(&self, lane_id: usize) -> (u64, u64) {
        let mut soft_age_us = self.v3_durable_confirm_soft_reject_age_us;
        let mut hard_age_us = self.v3_durable_confirm_hard_reject_age_us;
        if self.v3_durable_confirm_age_autotune_enabled {
            if let Some(value) = self
                .v3_durable_confirm_soft_reject_age_effective_us_per_lane
                .get(lane_id)
            {
                let tuned = value.load(Ordering::Relaxed);
                if tuned > 0 {
                    soft_age_us = tuned;
                }
            }
            if let Some(value) = self
                .v3_durable_confirm_hard_reject_age_effective_us_per_lane
                .get(lane_id)
            {
                let tuned = value.load(Ordering::Relaxed);
                if tuned > 0 {
                    hard_age_us = tuned;
                }
            }
        }
        if soft_age_us > 0 && hard_age_us > 0 && hard_age_us <= soft_age_us {
            hard_age_us = soft_age_us.saturating_add(1);
        }
        (soft_age_us, hard_age_us)
    }

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

fn parse_cpu_affinity_list_env(key: &str) -> Vec<usize> {
    let raw = match std::env::var(key) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut cpus = Vec::new();
    for part in raw.split(',') {
        let token = part.trim();
        if token.is_empty() {
            continue;
        }
        if let Some((lo, hi)) = token.split_once('-') {
            let start = lo.trim().parse::<usize>().ok();
            let end = hi.trim().parse::<usize>().ok();
            let (Some(start), Some(end)) = (start, end) else {
                continue;
            };
            if start <= end {
                cpus.extend(start..=end);
            } else {
                cpus.extend(end..=start);
            }
            continue;
        }
        if let Ok(cpu) = token.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus.sort_unstable();
    cpus.dedup();
    cpus
}

#[inline]
fn affinity_cpu_for_worker(cpus: &[usize], idx: usize) -> Option<usize> {
    if cpus.is_empty() {
        None
    } else {
        Some(cpus[idx % cpus.len()])
    }
}

#[cfg(target_os = "linux")]
fn pin_current_thread_cpu(cpu: usize) -> std::io::Result<()> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
    }
    let rc = unsafe {
        libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_cpu(_cpu: usize) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "thread affinity is only supported on Linux",
    ))
}

fn spawn_v3_runtime_thread<Fut>(
    name: String,
    cpu: Option<usize>,
    affinity_ok_total: Arc<AtomicU64>,
    affinity_err_total: Arc<AtomicU64>,
    fut: Fut,
) where
    Fut: Future<Output = ()> + Send + 'static,
{
    let thread_name = name.clone();
    let affinity_err_total_spawn = affinity_err_total.clone();
    let spawn_result = std::thread::Builder::new()
        .name(thread_name.clone())
        .spawn(move || {
            if let Some(cpu) = cpu {
                match pin_current_thread_cpu(cpu) {
                    Ok(()) => {
                        affinity_ok_total.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(err) => {
                        affinity_err_total_spawn.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!(thread = %thread_name, cpu = cpu, error = %err, "failed to pin v3 worker thread");
                    }
                }
            }
            match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt.block_on(fut),
                Err(err) => tracing::error!(thread = %thread_name, error = %err, "failed to build dedicated v3 runtime"),
            }
        });
    if let Err(err) = spawn_result {
        affinity_err_total.fetch_add(1, Ordering::Relaxed);
        tracing::error!(thread = %name, error = %err, "failed to spawn dedicated v3 worker thread");
    }
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

fn v3_durable_lane_replica_wal_path(lane_primary: &Path) -> PathBuf {
    PathBuf::from(format!("{}.replica", lane_primary.display()))
}

#[derive(Debug, Clone)]
struct V3RebuildRecord {
    status: V3ConfirmStatus,
    reason: Option<String>,
    at_ns: u64,
    account_id: String,
    position_symbol_key: Option<[u8; 8]>,
    position_delta_qty: Option<i64>,
    shard_id: Option<usize>,
}

#[derive(Debug, Default, Clone, Copy)]
struct V3RebuildStats {
    confirm_restored: u64,
    position_applied: u64,
    session_seq_seeded: u64,
    session_shard_seeded: u64,
}

fn parse_v3_u64_data_field(data: &serde_json::Value, key: &str) -> Option<u64> {
    data.get(key).and_then(|value| {
        value
            .as_u64()
            .or_else(|| value.as_i64().and_then(|v| (v >= 0).then_some(v as u64)))
            .or_else(|| value.as_str().and_then(|raw| raw.parse::<u64>().ok()))
    })
}

fn parse_v3_i64_data_field(data: &serde_json::Value, key: &str) -> Option<i64> {
    data.get(key).and_then(|value| {
        value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|v| i64::try_from(v).ok()))
            .or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok()))
    })
}

fn collect_v3_rebuild_records_from_reader<R: BufRead>(
    reader: R,
    max_lines: usize,
) -> (
    HashMap<(String, u64), V3RebuildRecord>,
    HashMap<String, u64>,
) {
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

    let mut records: HashMap<(String, u64), V3RebuildRecord> = HashMap::new();
    let mut max_seq_per_session: HashMap<String, u64> = HashMap::new();
    for event in ring {
        let Some(order_id) = event.order_id.as_deref() else {
            continue;
        };
        let Some((session_id, session_seq)) = parse_v3_order_id(order_id) else {
            continue;
        };
        max_seq_per_session
            .entry(session_id.clone())
            .and_modify(|max_seq| *max_seq = (*max_seq).max(session_seq))
            .or_insert(session_seq);
        let at_ns = event.at.saturating_mul(1_000_000);
        match event.event_type.as_str() {
            "V3DurableAccepted" => {
                let position_symbol_key =
                    parse_v3_u64_data_field(&event.data, "positionSymbolKey").map(u64::to_le_bytes);
                let position_delta_qty = parse_v3_i64_data_field(&event.data, "positionDeltaQty");
                let shard_id = parse_v3_u64_data_field(&event.data, "shardId")
                    .and_then(|value| usize::try_from(value).ok());
                records.insert(
                    (session_id, session_seq),
                    V3RebuildRecord {
                        status: V3ConfirmStatus::DurableAccepted,
                        reason: None,
                        at_ns,
                        account_id: event.account_id,
                        position_symbol_key,
                        position_delta_qty,
                        shard_id,
                    },
                );
            }
            "V3DurableRejected" => {
                let reason = event
                    .data
                    .get("reason")
                    .and_then(|v| v.as_str())
                    .unwrap_or("WAL_DURABILITY_FAILED")
                    .to_string();
                let shard_id = parse_v3_u64_data_field(&event.data, "shardId")
                    .and_then(|value| usize::try_from(value).ok());
                records.insert(
                    (session_id, session_seq),
                    V3RebuildRecord {
                        status: V3ConfirmStatus::DurableRejected,
                        reason: Some(reason),
                        at_ns,
                        account_id: event.account_id,
                        position_symbol_key: None,
                        position_delta_qty: None,
                        shard_id,
                    },
                );
            }
            _ => {}
        }
    }
    (records, max_seq_per_session)
}

fn rebuild_v3_confirm_store_from_reader<R: BufRead>(
    confirm_store: &V3ConfirmStore,
    reader: R,
    max_lines: usize,
) -> u64 {
    let (records, _max_seq_per_session) = collect_v3_rebuild_records_from_reader(reader, max_lines);
    let mut restored = 0u64;
    for ((session_id, session_seq), record) in records {
        match record.status {
            V3ConfirmStatus::DurableAccepted => {
                confirm_store.mark_durable_accepted(&session_id, session_seq, record.at_ns);
                restored = restored.saturating_add(1);
            }
            V3ConfirmStatus::DurableRejected => {
                let reason = record.reason.as_deref().unwrap_or("WAL_DURABILITY_FAILED");
                confirm_store.mark_durable_rejected(&session_id, session_seq, reason, record.at_ns);
                restored = restored.saturating_add(1);
            }
            _ => {}
        }
    }
    restored
}

fn rebuild_v3_runtime_state_from_reader<R: BufRead>(
    state: &AppState,
    reader: R,
    max_lines: usize,
) -> V3RebuildStats {
    let (records, max_seq_per_session) = collect_v3_rebuild_records_from_reader(reader, max_lines);
    let mut stats = V3RebuildStats::default();

    for ((session_id, session_seq), record) in records {
        match record.status {
            V3ConfirmStatus::DurableAccepted => {
                state
                    .v3_confirm_store
                    .mark_durable_accepted(&session_id, session_seq, record.at_ns);
                stats.confirm_restored = stats.confirm_restored.saturating_add(1);
                if let (Some(symbol_key), Some(delta_qty)) =
                    (record.position_symbol_key, record.position_delta_qty)
                {
                    if delta_qty != 0 {
                        let account_id = state.intern_v3_account_id(&record.account_id);
                        state.apply_v3_position_delta(account_id, symbol_key, delta_qty);
                        stats.position_applied = stats.position_applied.saturating_add(1);
                    }
                }
            }
            V3ConfirmStatus::DurableRejected => {
                let reason = record.reason.as_deref().unwrap_or("WAL_DURABILITY_FAILED");
                state.v3_confirm_store.mark_durable_rejected(
                    &session_id,
                    session_seq,
                    reason,
                    record.at_ns,
                );
                stats.confirm_restored = stats.confirm_restored.saturating_add(1);
            }
            _ => {}
        }
        if let Some(shard_id) = record.shard_id {
            if state.v3_ingress.seed_session_shard(&session_id, shard_id) {
                stats.session_shard_seeded = stats.session_shard_seeded.saturating_add(1);
            }
        }
    }

    for (session_id, max_seq) in max_seq_per_session {
        if state
            .v3_ingress
            .seed_next_seq_floor(&session_id, max_seq.saturating_add(1))
        {
            stats.session_seq_seeded = stats.session_seq_seeded.saturating_add(1);
        }
    }

    stats
}

fn rebuild_v3_confirm_store_from_wal_path(
    state: &AppState,
    path: &Path,
    max_lines: usize,
) -> V3RebuildStats {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return V3RebuildStats::default(),
    };
    let reader = BufReader::new(file);
    rebuild_v3_runtime_state_from_reader(state, reader, max_lines)
}

fn rebuild_v3_confirm_store_from_wal(state: &AppState, max_lines: usize) -> V3RebuildStats {
    let mut aggregate = V3RebuildStats::default();
    for path in state.v3_confirm_rebuild_paths.iter() {
        let stats = rebuild_v3_confirm_store_from_wal_path(state, path, max_lines);
        aggregate.confirm_restored = aggregate
            .confirm_restored
            .saturating_add(stats.confirm_restored);
        aggregate.position_applied = aggregate
            .position_applied
            .saturating_add(stats.position_applied);
        aggregate.session_seq_seeded = aggregate
            .session_seq_seeded
            .saturating_add(stats.session_seq_seeded);
        aggregate.session_shard_seeded = aggregate
            .session_shard_seeded
            .saturating_add(stats.session_shard_seeded);
    }
    aggregate
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
    durable_rx: Option<UnboundedReceiver<AuditDurableNotification>>,
    sharded_store: Arc<ShardedOrderStore>,
    order_id_map: Arc<OrderIdMap>,
) -> anyhow::Result<()> {
    let configured_shard_count = std::env::var("ORDER_STORE_SHARDS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(sharded_store.shard_count());
    if configured_shard_count != sharded_store.shard_count() {
        info!(
            configured = configured_shard_count,
            effective = sharded_store.shard_count(),
            "ORDER_STORE_SHARDS differs from injected store shard count; using injected store"
        );
    }
    info!(
        "ShardedOrderStore configured (shards={})",
        sharded_store.shard_count()
    );

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
    let v3_shard_affinity_cpus = parse_cpu_affinity_list_env("V3_SHARD_AFFINITY_CPUS");
    let v3_durable_affinity_cpus = parse_cpu_affinity_list_env("V3_DURABLE_AFFINITY_CPUS");
    let v3_tcp_server_affinity_cpus = parse_cpu_affinity_list_env("V3_TCP_SERVER_AFFINITY_CPUS");
    let v3_shard_affinity_layout = Arc::new(
        (0..v3_shard_count)
            .map(|idx| {
                affinity_cpu_for_worker(&v3_shard_affinity_cpus, idx)
                    .map(|cpu| cpu as i64)
                    .unwrap_or(-1)
            })
            .collect::<Vec<_>>(),
    );
    let v3_durable_affinity_layout = Arc::new(
        (0..v3_durable_lane_count)
            .map(|idx| {
                affinity_cpu_for_worker(&v3_durable_affinity_cpus, idx)
                    .map(|cpu| cpu as i64)
                    .unwrap_or(-1)
            })
            .collect::<Vec<_>>(),
    );
    let v3_tcp_server_affinity_cpu = v3_tcp_server_affinity_cpus
        .first()
        .copied()
        .map(|cpu| cpu as i64)
        .unwrap_or(-1);
    let v3_tsc_timing_enable = parse_bool_env("V3_TSC_TIMING_ENABLE").unwrap_or(false);
    let v3_tsc_mismatch_threshold_pct = std::env::var("V3_TSC_MISMATCH_THRESHOLD_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(|v| v.clamp(1, 500))
        .unwrap_or(20);
    let v3_hotpath_histogram_sample_rate = std::env::var("V3_HOTPATH_HISTOGRAM_SAMPLE_RATE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(1);
    let v3_dedicated_worker_runtime =
        parse_bool_env("V3_DEDICATED_WORKER_RUNTIME").unwrap_or(true);
    let v3_tsc_clock = if v3_tsc_timing_enable {
        match RdtscpClock::detect() {
            Some(clock) => Some(Arc::new(clock)),
            None => {
                tracing::warn!(
                    "V3_TSC_TIMING_ENABLE=true but rdtscp clock detection failed; fallback to now_nanos only"
                );
                None
            }
        }
    } else {
        None
    };
    let v3_tsc_runtime_enabled = Arc::new(AtomicBool::new(v3_tsc_clock.is_some()));
    let v3_durable_worker_batch_max = std::env::var("V3_DURABLE_WORKER_BATCH_MAX")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(24);
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
        .unwrap_or(30);
    let v3_durable_worker_receipt_timeout_us =
        std::env::var("V3_DURABLE_WORKER_RECEIPT_TIMEOUT_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20_000_000);
    let v3_durable_replica_enabled = parse_bool_env("V3_DURABLE_REPLICA_ENABLED").unwrap_or(false);
    let v3_durable_replica_required = v3_durable_replica_enabled
        && parse_bool_env("V3_DURABLE_REPLICA_REQUIRED").unwrap_or(false);
    let v3_durable_replica_receipt_timeout_us =
        std::env::var("V3_DURABLE_REPLICA_RECEIPT_TIMEOUT_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(v3_durable_worker_receipt_timeout_us.max(1));
    let v3_durable_worker_max_inflight_receipts =
        std::env::var("V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(8192);
    let v3_durable_worker_max_inflight_receipts_global =
        std::env::var("V3_DURABLE_WORKER_MAX_INFLIGHT_RECEIPTS_GLOBAL")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(
                v3_durable_worker_max_inflight_receipts
                    .saturating_mul(v3_durable_lane_count.max(1))
                    .max(v3_durable_worker_max_inflight_receipts),
            );
    let mut v3_durable_worker_inflight_soft_cap_pct =
        std::env::var("V3_DURABLE_WORKER_INFLIGHT_SOFT_CAP_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.clamp(1, 100))
            .unwrap_or(50);
    let mut v3_durable_worker_inflight_hard_cap_pct =
        std::env::var("V3_DURABLE_WORKER_INFLIGHT_HARD_CAP_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.clamp(1, 100))
            .unwrap_or(25);
    if v3_durable_worker_inflight_hard_cap_pct > v3_durable_worker_inflight_soft_cap_pct {
        v3_durable_worker_inflight_hard_cap_pct = v3_durable_worker_inflight_soft_cap_pct;
    }
    if v3_durable_worker_inflight_soft_cap_pct == 0 {
        v3_durable_worker_inflight_soft_cap_pct = 1;
    }
    let mut v3_durable_worker_batch_wait_min_us =
        std::env::var("V3_DURABLE_WORKER_BATCH_WAIT_MIN_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or((v3_durable_worker_batch_wait_us / 2).max(15));
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
    let v3_durable_worker_pressure_cfg =
        V3DurableWorkerPressureConfig::from_env(v3_durable_worker_inflight_hard_cap_pct);
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
    let v3_durable_ack_path_guard_enabled =
        parse_bool_env("V3_DURABLE_ACK_PATH_GUARD_ENABLED").unwrap_or(true);
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
    let v3_durable_confirm_age_autotune_enabled =
        parse_bool_env("V3_DURABLE_CONFIRM_AGE_AUTOTUNE").unwrap_or(true);
    let v3_durable_confirm_age_autotune_alpha_pct =
        std::env::var("V3_DURABLE_CONFIRM_AGE_AUTOTUNE_ALPHA_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20)
            .clamp(1, 100);
    let v3_durable_fsync_ewma_alpha_pct = std::env::var("V3_DURABLE_FSYNC_EWMA_ALPHA_PCT")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(30)
        .clamp(1, 100);
    let v3_durable_confirm_age_fsync_linked =
        parse_bool_env("V3_DURABLE_CONFIRM_AGE_FSYNC_LINKED").unwrap_or(true);
    let v3_durable_confirm_age_fsync_soft_ref_us =
        std::env::var("V3_DURABLE_CONFIRM_AGE_FSYNC_SOFT_REF_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(v3_durable_admission_soft_fsync_p99_us.max(1))
            .max(1);
    let mut v3_durable_confirm_age_fsync_hard_ref_us =
        std::env::var("V3_DURABLE_CONFIRM_AGE_FSYNC_HARD_REF_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(v3_durable_admission_hard_fsync_p99_us.max(1))
            .max(v3_durable_confirm_age_fsync_soft_ref_us);
    if v3_durable_confirm_age_fsync_hard_ref_us <= v3_durable_confirm_age_fsync_soft_ref_us {
        v3_durable_confirm_age_fsync_hard_ref_us =
            v3_durable_confirm_age_fsync_soft_ref_us.saturating_add(1);
    }
    let v3_durable_confirm_age_fsync_max_relax_pct =
        std::env::var("V3_DURABLE_CONFIRM_AGE_FSYNC_MAX_RELAX_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(100)
            .clamp(0, 300);
    let v3_durable_confirm_guard_soft_slack_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_SOFT_SLACK_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(20)
            .min(50);
    let v3_durable_confirm_guard_hard_slack_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_HARD_SLACK_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(40)
            .min(100);
    let v3_durable_confirm_guard_soft_requires_admission =
        parse_bool_env("V3_DURABLE_CONFIRM_GUARD_SOFT_REQUIRES_ADMISSION").unwrap_or(true);
    let v3_durable_confirm_guard_hard_requires_admission =
        parse_bool_env("V3_DURABLE_CONFIRM_GUARD_HARD_REQUIRES_ADMISSION").unwrap_or(true);
    let v3_durable_confirm_guard_min_queue_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_MIN_QUEUE_PCT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0)
            .clamp(0.0, 100.0);
    let v3_durable_confirm_guard_min_inflight_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_MIN_INFLIGHT_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0)
            .min(100);
    let v3_durable_confirm_guard_secondary_required =
        parse_bool_env("V3_DURABLE_CONFIRM_GUARD_SECONDARY_REQUIRED").unwrap_or(true);
    let v3_durable_confirm_guard_min_backlog_per_sec =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_MIN_BACKLOG_PER_SEC")
            .ok()
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
            .max(0);
    let v3_durable_confirm_guard_soft_sustain_ticks =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_SOFT_SUSTAIN_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2)
            .max(1);
    let v3_durable_confirm_guard_hard_sustain_ticks =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_HARD_SUSTAIN_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(2)
            .max(1);
    let v3_durable_confirm_guard_recover_ticks =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_RECOVER_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(3)
            .max(1);
    let v3_durable_confirm_guard_recover_hysteresis_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_RECOVER_HYSTERESIS_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(85)
            .clamp(50, 99);
    let v3_durable_confirm_guard_autotune_enabled =
        parse_bool_env("V3_DURABLE_CONFIRM_GUARD_AUTOTUNE").unwrap_or(true);
    let v3_durable_confirm_guard_autotune_low_pressure_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_LOW_PRESSURE_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(35)
            .min(100);
    let v3_durable_confirm_guard_autotune_high_pressure_pct =
        std::env::var("V3_DURABLE_CONFIRM_GUARD_AUTOTUNE_HIGH_PRESSURE_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(80)
            .min(100)
            .max(v3_durable_confirm_guard_autotune_low_pressure_pct);
    let default_soft_min_us = if v3_durable_confirm_soft_reject_age_us > 0 {
        (v3_durable_confirm_soft_reject_age_us.saturating_mul(7) / 10).max(10_000)
    } else {
        0
    };
    let default_soft_max_us = if v3_durable_confirm_soft_reject_age_us > 0 {
        v3_durable_confirm_soft_reject_age_us
    } else {
        0
    };
    let mut v3_durable_confirm_soft_reject_age_min_us =
        std::env::var("V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_MIN_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_soft_min_us);
    let mut v3_durable_confirm_soft_reject_age_max_us =
        std::env::var("V3_DURABLE_CONFIRM_SOFT_REJECT_AGE_MAX_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_soft_max_us);
    if v3_durable_confirm_soft_reject_age_us > 0 {
        // tighten-only: autotune must not relax above base soft threshold.
        v3_durable_confirm_soft_reject_age_min_us = v3_durable_confirm_soft_reject_age_min_us
            .max(1)
            .min(v3_durable_confirm_soft_reject_age_us);
        v3_durable_confirm_soft_reject_age_max_us = v3_durable_confirm_soft_reject_age_max_us
            .max(v3_durable_confirm_soft_reject_age_min_us)
            .min(v3_durable_confirm_soft_reject_age_us);
    }
    let default_hard_min_us = if v3_durable_confirm_hard_reject_age_us > 0 {
        (v3_durable_confirm_hard_reject_age_us.saturating_mul(75) / 100).max(10_000)
    } else {
        0
    };
    let default_hard_max_us = if v3_durable_confirm_hard_reject_age_us > 0 {
        v3_durable_confirm_hard_reject_age_us
    } else {
        0
    };
    let mut v3_durable_confirm_hard_reject_age_min_us =
        std::env::var("V3_DURABLE_CONFIRM_HARD_REJECT_AGE_MIN_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_hard_min_us);
    let mut v3_durable_confirm_hard_reject_age_max_us =
        std::env::var("V3_DURABLE_CONFIRM_HARD_REJECT_AGE_MAX_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_hard_max_us);
    if v3_durable_confirm_hard_reject_age_us > 0 {
        // tighten-only: autotune must not relax above base hard threshold.
        v3_durable_confirm_hard_reject_age_min_us = v3_durable_confirm_hard_reject_age_min_us
            .max(1)
            .min(v3_durable_confirm_hard_reject_age_us);
        v3_durable_confirm_hard_reject_age_max_us = v3_durable_confirm_hard_reject_age_max_us
            .max(v3_durable_confirm_hard_reject_age_min_us)
            .min(v3_durable_confirm_hard_reject_age_us);
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
    let new_shard_u64_counters = || {
        Arc::new(
            (0..v3_shard_count.max(1))
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
    let v3_durable_confirm_soft_reject_age_effective_us_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(v3_durable_confirm_soft_reject_age_us)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_hard_reject_age_effective_us_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(v3_durable_confirm_hard_reject_age_us)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_hourly_pressure_ewma_per_lane = Arc::new(
        (0..v3_durable_lane_count.saturating_mul(24))
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_guard_soft_sustain_ticks_effective_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(v3_durable_confirm_guard_soft_sustain_ticks)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_guard_hard_sustain_ticks_effective_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(v3_durable_confirm_guard_hard_sustain_ticks)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_guard_recover_ticks_effective_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(v3_durable_confirm_guard_recover_ticks)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_guard_soft_armed_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(0)))
            .collect::<Vec<_>>(),
    );
    let v3_durable_confirm_guard_hard_armed_per_lane = Arc::new(
        (0..v3_durable_lane_count)
            .map(|_| Arc::new(AtomicU64::new(0)))
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
        sharded_store,
        order_id_map,
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
        v3_accepted_total_per_shard: new_shard_u64_counters(),
        v3_rejected_soft_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_soft_total_per_shard: new_shard_u64_counters(),
        v3_rejected_hard_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_hard_total_per_shard: new_shard_u64_counters(),
        v3_rejected_killed_total: Arc::new(AtomicU64::new(0)),
        v3_rejected_killed_total_per_shard: new_shard_u64_counters(),
        v3_kill_recovered_total: Arc::new(AtomicU64::new(0)),
        v3_loss_suspect_total: Arc::new(AtomicU64::new(0)),
        v3_session_killed_total: Arc::new(AtomicU64::new(0)),
        v3_shard_killed_total: Arc::new(AtomicU64::new(0)),
        v3_global_killed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_ingress: Arc::clone(&v3_durable_ingress),
        v3_durable_accepted_total: Arc::new(AtomicU64::new(0)),
        v3_durable_accepted_total_per_lane: new_lane_u64_counters(),
        v3_durable_rejected_total: Arc::new(AtomicU64::new(0)),
        v3_durable_rejected_total_per_lane: new_lane_u64_counters(),
        v3_live_ack_hist: Arc::new(LatencyHistogram::new()),
        v3_live_ack_hist_ns: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_hist: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_hist_ns: Arc::new(LatencyHistogram::new()),
        v3_live_ack_accepted_tsc_hist_ns: Arc::new(LatencyHistogram::new()),
        v3_tsc_clock: v3_tsc_clock.clone(),
        v3_tsc_runtime_enabled: Arc::clone(&v3_tsc_runtime_enabled),
        v3_tsc_invariant: v3_tsc_clock
            .as_ref()
            .map(|clock| clock.invariant_tsc())
            .unwrap_or(false),
        v3_tsc_hz: v3_tsc_clock
            .as_ref()
            .map(|clock| clock.hz())
            .unwrap_or(0),
        v3_tsc_mismatch_threshold_pct,
        v3_tsc_fallback_total: Arc::new(AtomicU64::new(0)),
        v3_tsc_cross_core_total: Arc::new(AtomicU64::new(0)),
        v3_tsc_mismatch_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_append_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_fsync_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_wal_fsync_hist_per_lane,
        v3_durable_fsync_p99_cached_us: Arc::new(AtomicU64::new(
            v3_durable_admission_soft_fsync_p99_us,
        )),
        v3_durable_fsync_p99_cached_us_per_lane: Arc::new(
            (0..v3_durable_lane_count)
                .map(|_| Arc::new(AtomicU64::new(v3_durable_admission_soft_fsync_p99_us)))
                .collect::<Vec<_>>(),
        ),
        v3_durable_fsync_ewma_alpha_pct,
        v3_durable_worker_loop_hist: Arc::new(LatencyHistogram::new()),
        v3_durable_worker_loop_hist_per_lane,
        v3_durable_worker_batch_min,
        v3_durable_worker_batch_max,
        v3_durable_worker_batch_wait_min_us,
        v3_durable_worker_batch_wait_us,
        v3_durable_worker_receipt_timeout_us,
        v3_durable_replica_enabled,
        v3_durable_replica_required,
        v3_durable_replica_receipt_timeout_us,
        v3_durable_worker_max_inflight_receipts,
        v3_durable_worker_max_inflight_receipts_global,
        v3_durable_worker_inflight_soft_cap_pct,
        v3_durable_worker_inflight_hard_cap_pct,
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
        v3_durable_ack_path_guard_enabled,
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
        v3_durable_replica_append_total: Arc::new(AtomicU64::new(0)),
        v3_durable_replica_write_error_total: Arc::new(AtomicU64::new(0)),
        v3_durable_replica_receipt_timeout_total: Arc::new(AtomicU64::new(0)),
        v3_durable_receipt_timeout_total: Arc::new(AtomicU64::new(0)),
        v3_durable_receipt_inflight_per_lane: new_lane_u64_counters(),
        v3_durable_receipt_inflight_max_per_lane: new_lane_u64_counters(),
        v3_durable_receipt_inflight: Arc::new(AtomicU64::new(0)),
        v3_durable_receipt_inflight_max: Arc::new(AtomicU64::new(0)),
        v3_durable_pressure_pct_per_lane: new_lane_u64_counters(),
        v3_durable_dynamic_cap_pct_per_lane: new_lane_u64_counters(),
        v3_soft_reject_pct,
        v3_hard_reject_pct,
        v3_kill_reject_pct,
        v3_thread_affinity_apply_success_total: Arc::new(AtomicU64::new(0)),
        v3_thread_affinity_apply_failure_total: Arc::new(AtomicU64::new(0)),
        v3_shard_affinity_cpu: Arc::clone(&v3_shard_affinity_layout),
        v3_durable_affinity_cpu: Arc::clone(&v3_durable_affinity_layout),
        v3_tcp_server_affinity_cpu,
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
        v3_replay_position_applied_total: Arc::new(AtomicU64::new(0)),
        v3_replay_session_seq_seeded_total: Arc::new(AtomicU64::new(0)),
        v3_replay_session_shard_seeded_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_soft_reject_age_us,
        v3_durable_confirm_hard_reject_age_us,
        v3_durable_confirm_guard_soft_slack_pct,
        v3_durable_confirm_guard_hard_slack_pct,
        v3_durable_confirm_guard_soft_requires_admission,
        v3_durable_confirm_guard_hard_requires_admission,
        v3_durable_confirm_guard_secondary_required,
        v3_durable_confirm_guard_min_queue_pct,
        v3_durable_confirm_guard_min_inflight_pct,
        v3_durable_confirm_guard_min_backlog_per_sec,
        v3_durable_confirm_guard_soft_sustain_ticks,
        v3_durable_confirm_guard_hard_sustain_ticks,
        v3_durable_confirm_guard_recover_ticks,
        v3_durable_confirm_guard_recover_hysteresis_pct,
        v3_durable_confirm_guard_autotune_enabled,
        v3_durable_confirm_guard_autotune_low_pressure_pct,
        v3_durable_confirm_guard_autotune_high_pressure_pct,
        v3_durable_confirm_guard_soft_sustain_ticks_effective_per_lane,
        v3_durable_confirm_guard_hard_sustain_ticks_effective_per_lane,
        v3_durable_confirm_guard_recover_ticks_effective_per_lane,
        v3_durable_confirm_guard_soft_armed_per_lane,
        v3_durable_confirm_guard_hard_armed_per_lane,
        v3_durable_confirm_age_soft_reject_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_soft_reject_skipped_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_skipped_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_soft_reject_skipped_unarmed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_skipped_unarmed_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_soft_reject_skipped_low_load_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_hard_reject_skipped_low_load_total: Arc::new(AtomicU64::new(0)),
        v3_durable_confirm_age_autotune_enabled,
        v3_durable_confirm_age_autotune_alpha_pct,
        v3_durable_confirm_age_fsync_linked,
        v3_durable_confirm_age_fsync_soft_ref_us,
        v3_durable_confirm_age_fsync_hard_ref_us,
        v3_durable_confirm_age_fsync_max_relax_pct,
        v3_durable_confirm_soft_reject_age_min_us,
        v3_durable_confirm_soft_reject_age_max_us,
        v3_durable_confirm_hard_reject_age_min_us,
        v3_durable_confirm_hard_reject_age_max_us,
        v3_durable_confirm_soft_reject_age_effective_us_per_lane,
        v3_durable_confirm_hard_reject_age_effective_us_per_lane,
        v3_durable_confirm_hourly_pressure_ewma_per_lane,
        v3_risk_profile,
        v3_risk_margin_mode,
        v3_risk_loops,
        v3_risk_strict_symbols,
        v3_risk_max_order_qty,
        v3_risk_max_notional,
        v3_risk_daily_notional_limit,
        v3_risk_max_abs_position_qty,
        v3_symbol_limits,
        v3_session_id_intern: Arc::new(dashmap::DashMap::new()),
        v3_account_id_intern: Arc::new(dashmap::DashMap::new()),
        v3_account_daily_notional: Arc::new(dashmap::DashMap::new()),
        v3_account_symbol_position: Arc::new(dashmap::DashMap::new()),
        v3_hotpath_histogram_sample_rate,
        v3_hotpath_sample_cursor: Arc::new(AtomicU64::new(0)),
        v3_stage_parse_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_position_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_margin_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_risk_limits_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_enqueue_hist: Arc::new(LatencyHistogram::new()),
        v3_stage_serialize_hist: Arc::new(LatencyHistogram::new()),
    };

    info!(
        shard_affinity = ?v3_shard_affinity_cpus,
        durable_affinity = ?v3_durable_affinity_cpus,
        tcp_server_affinity = ?v3_tcp_server_affinity_cpus,
        tsc_enabled = v3_tsc_runtime_enabled.load(Ordering::Relaxed),
        tsc_hz = state.v3_tsc_hz,
        tsc_invariant = state.v3_tsc_invariant,
        hotpath_histogram_sample_rate = state.v3_hotpath_histogram_sample_rate,
        dedicated_worker_runtime = v3_dedicated_worker_runtime,
        "v3 worker topology tuning"
    );

    if v3_confirm_rebuild_on_start {
        let rebuild_t0 = Instant::now();
        let rebuild_stats = rebuild_v3_confirm_store_from_wal(&state, v3_confirm_rebuild_max_lines);
        let elapsed_ms = rebuild_t0.elapsed().as_millis() as u64;
        state
            .v3_confirm_rebuild_restored_total
            .store(rebuild_stats.confirm_restored, Ordering::Relaxed);
        state
            .v3_confirm_rebuild_elapsed_ms
            .store(elapsed_ms, Ordering::Relaxed);
        state
            .v3_replay_position_applied_total
            .store(rebuild_stats.position_applied, Ordering::Relaxed);
        state
            .v3_replay_session_seq_seeded_total
            .store(rebuild_stats.session_seq_seeded, Ordering::Relaxed);
        state
            .v3_replay_session_shard_seeded_total
            .store(rebuild_stats.session_shard_seeded, Ordering::Relaxed);
        info!(
            restored = rebuild_stats.confirm_restored,
            position_applied = rebuild_stats.position_applied,
            session_seq_seeded = rebuild_stats.session_seq_seeded,
            session_shard_seeded = rebuild_stats.session_shard_seeded,
            elapsed_ms = elapsed_ms,
            max_lines = v3_confirm_rebuild_max_lines,
            "v3 runtime state rebuilt from WAL"
        );
    }

    for (shard_id, v3_rx) in v3_rxs.into_iter().enumerate() {
        let writer_state = state.clone();
        let affinity_cpu = affinity_cpu_for_worker(&v3_shard_affinity_cpus, shard_id);
        if v3_dedicated_worker_runtime {
            spawn_v3_runtime_thread(
                format!("v3-shard-{shard_id}"),
                affinity_cpu,
                Arc::clone(&state.v3_thread_affinity_apply_success_total),
                Arc::clone(&state.v3_thread_affinity_apply_failure_total),
                async move {
                    run_v3_single_writer(shard_id, v3_rx, writer_state).await;
                },
            );
        } else {
            tokio::spawn(async move {
                run_v3_single_writer(shard_id, v3_rx, writer_state).await;
            });
        }
    }
    for (lane_id, v3_durable_rx) in v3_durable_rxs.into_iter().enumerate() {
        let durable_state = state.clone();
        let durable_batch_max = durable_state.v3_durable_worker_batch_max;
        let durable_batch_wait_us = durable_state.v3_durable_worker_batch_wait_us;
        let durable_batch_adaptive_cfg = v3_durable_worker_batch_adaptive_cfg;
        let durable_pressure_cfg = v3_durable_worker_pressure_cfg;
        let affinity_cpu = affinity_cpu_for_worker(&v3_durable_affinity_cpus, lane_id);
        if v3_dedicated_worker_runtime {
            spawn_v3_runtime_thread(
                format!("v3-durable-{lane_id}"),
                affinity_cpu,
                Arc::clone(&state.v3_thread_affinity_apply_success_total),
                Arc::clone(&state.v3_thread_affinity_apply_failure_total),
                async move {
                    run_v3_durable_worker(
                        lane_id,
                        v3_durable_rx,
                        durable_state,
                        durable_batch_max,
                        durable_batch_wait_us,
                        durable_batch_adaptive_cfg,
                        durable_pressure_cfg,
                    )
                    .await;
                },
            );
        } else {
            tokio::spawn(async move {
                run_v3_durable_worker(
                    lane_id,
                    v3_durable_rx,
                    durable_state,
                    durable_batch_max,
                    durable_batch_wait_us,
                    durable_batch_adaptive_cfg,
                    durable_pressure_cfg,
                )
                .await;
            });
        }
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

    let v3_http_enable = parse_bool_env("V3_HTTP_ENABLE");
    let v3_http_ingress_enable = parse_bool_env("V3_HTTP_INGRESS_ENABLE")
        .or(v3_http_enable)
        .unwrap_or(true);
    let v3_http_confirm_enable = parse_bool_env("V3_HTTP_CONFIRM_ENABLE")
        .or(v3_http_enable)
        .unwrap_or(true);
    let v3_tcp_enable = parse_bool_env("V3_TCP_ENABLE").unwrap_or(false);
    let v3_tcp_port = std::env::var("V3_TCP_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(0);
    info!(
        v3_http_ingress_enable = v3_http_ingress_enable,
        v3_http_confirm_enable = v3_http_confirm_enable,
        v3_tcp_enable = v3_tcp_enable,
        v3_tcp_port = v3_tcp_port,
        "v3 ingress transport settings"
    );
    if v3_tcp_enable && v3_tcp_port > 0 {
        let v3_tcp_state = state.clone();
        let affinity_cpu = v3_tcp_server_affinity_cpus.first().copied();
        if v3_dedicated_worker_runtime && affinity_cpu.is_some() {
            spawn_v3_runtime_thread(
                "v3-tcp-ingress".to_string(),
                affinity_cpu,
                Arc::clone(&state.v3_thread_affinity_apply_success_total),
                Arc::clone(&state.v3_thread_affinity_apply_failure_total),
                async move {
                    if let Err(err) = run_v3_tcp_server(v3_tcp_port, v3_tcp_state).await {
                        tracing::error!(error = %err, "v3 tcp server exited");
                    }
                },
            );
        } else {
            if !v3_dedicated_worker_runtime && affinity_cpu.is_some() {
                tracing::warn!(
                    "V3_TCP_SERVER_AFFINITY_CPUS is ignored when V3_DEDICATED_WORKER_RUNTIME=false"
                );
            }
            tokio::spawn(async move {
                if let Err(err) = run_v3_tcp_server(v3_tcp_port, v3_tcp_state).await {
                    tracing::error!(error = %err, "v3 tcp server exited");
                }
            });
        }
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
        .route("/orders/{order_id}/replace", post(handle_replace_order))
        .route("/v2/orders/{order_id}/replace", post(handle_replace_order))
        .route("/orders/{order_id}/amend", post(handle_amend_order))
        .route("/v2/orders/{order_id}/amend", post(handle_amend_order))
        .route("/orders/{order_id}/events", get(handle_order_events))
        .route("/accounts/{account_id}/events", get(handle_account_events))
        .route("/orders/{order_id}/stream", get(handle_order_stream))
        .route("/stream", get(handle_account_stream))
        .route("/audit/verify", get(handle_audit_verify))
        .route("/audit/anchor", get(handle_audit_anchor))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics));
    let mut app = app_base;
    if v3_http_ingress_enable {
        app = app.route("/v3/orders", post(handle_order_v3));
    }
    if v3_http_confirm_enable {
        app = app.route(
            "/v3/orders/{session_id}/{session_seq}",
            get(handle_get_order_v3),
        );
    }
    let app = app.layer(CorsLayer::permissive()).with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    info!("HTTP server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}

async fn run_v3_tcp_server(port: u16, state: AppState) -> anyhow::Result<()> {
    let addr = format!("0.0.0.0:{}", port);
    let listener = TcpListener::bind(&addr).await?;
    let tcp_busy_poll_us = std::env::var("V3_TCP_BUSY_POLL_US")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(0);
    #[cfg(target_os = "linux")]
    if tcp_busy_poll_us > 0 {
        info!(
            busy_poll_us = tcp_busy_poll_us,
            "v3 tcp busy-poll is enabled (SO_BUSY_POLL)"
        );
    }
    #[cfg(not(target_os = "linux"))]
    if tcp_busy_poll_us > 0 {
        info!(
            busy_poll_us = tcp_busy_poll_us,
            "V3_TCP_BUSY_POLL_US is set but ignored on non-Linux"
        );
    }
    info!("v3 TCP server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        let conn_state = state.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_v3_tcp_connection(socket, conn_state, tcp_busy_poll_us).await {
                tracing::warn!(peer = %peer, error = %err, "v3 tcp connection error");
            }
        });
    }
}

#[cfg(target_os = "linux")]
static V3_TCP_BUSY_POLL_WARNED: AtomicBool = AtomicBool::new(false);

#[cfg(target_os = "linux")]
fn configure_v3_tcp_busy_poll(socket: &tokio::net::TcpStream, busy_poll_us: u32) {
    if busy_poll_us == 0 {
        return;
    }
    use std::os::fd::AsRawFd;
    let fd = socket.as_raw_fd();
    let value: libc::c_int = busy_poll_us.min(libc::c_int::MAX as u32) as libc::c_int;
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_BUSY_POLL,
            (&value as *const libc::c_int).cast::<libc::c_void>(),
            std::mem::size_of_val(&value) as libc::socklen_t,
        )
    };
    if rc != 0 && !V3_TCP_BUSY_POLL_WARNED.swap(true, Ordering::Relaxed) {
        let err = std::io::Error::last_os_error();
        tracing::warn!(error = %err, busy_poll_us = value, "failed to set SO_BUSY_POLL");
    }
}

#[cfg(not(target_os = "linux"))]
fn configure_v3_tcp_busy_poll(_socket: &tokio::net::TcpStream, _busy_poll_us: u32) {}

async fn handle_v3_tcp_connection(
    mut socket: tokio::net::TcpStream,
    state: AppState,
    tcp_busy_poll_us: u32,
) -> anyhow::Result<()> {
    socket.set_nodelay(true)?;
    configure_v3_tcp_busy_poll(&socket, tcp_busy_poll_us);
    let auth_cache_enabled = parse_bool_env("V3_TCP_AUTH_CACHE_ENABLE").unwrap_or(true);
    let sticky_auth_context = parse_bool_env("V3_TCP_AUTH_STICKY_CONTEXT").unwrap_or(true);
    let auth_cache_capacity = std::env::var("V3_TCP_AUTH_CACHE_CAPACITY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(256);
    let mut auth_cache: HashMap<String, crate::auth::Principal> = if auth_cache_enabled {
        HashMap::with_capacity(auth_cache_capacity.min(1024))
    } else {
        HashMap::new()
    };
    let mut sticky_token = String::new();
    let mut sticky_principal: Option<crate::auth::Principal> = None;
    let mut req = [0u8; V3_TCP_REQUEST_SIZE];
    loop {
        match socket.read_exact(&mut req).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let t0 = gateway_core::now_nanos();
        let resp = match decode_v3_tcp_request(&req) {
            Ok(decoded) => {
                let principal = if sticky_auth_context
                    && sticky_principal.is_some()
                    && sticky_token == decoded.jwt_token
                {
                    // 接続内で同一トークンが継続する場合は JWT 検証/HashMap 探索を避ける。
                    Ok(sticky_principal.as_ref().expect("checked is_some"))
                } else if auth_cache_enabled {
                    let mut auth_error: Option<(StatusCode, u32)> = None;
                    if !auth_cache.contains_key(decoded.jwt_token) {
                        match authenticate_v3_tcp_token(&state, decoded.jwt_token) {
                            Ok(principal) => {
                                if auth_cache.len() >= auth_cache_capacity {
                                    auth_cache.clear();
                                }
                                auth_cache.insert(decoded.jwt_token.to_string(), principal);
                            }
                            Err(err) => {
                                if sticky_auth_context {
                                    sticky_principal = None;
                                    sticky_token.clear();
                                }
                                auth_error = Some(err);
                            }
                        }
                    }
                    if let Some(err) = auth_error {
                        Err(err)
                    } else {
                        match auth_cache.get(decoded.jwt_token) {
                            Some(cached) => Ok(cached),
                            None => {
                                Err((StatusCode::UNAUTHORIZED, orders::V3_TCP_REASON_AUTH_INVALID))
                            }
                        }
                    }
                } else {
                    match authenticate_v3_tcp_token(&state, decoded.jwt_token) {
                        Ok(principal) => {
                            sticky_principal = Some(principal);
                            Ok(sticky_principal.as_ref().expect("set above"))
                        }
                        Err(err) => Err(err),
                    }
                };
                match principal {
                    Ok(principal) => {
                        let (status, body) = process_order_v3_hot_path_tcp(
                            &state,
                            &principal.account_id,
                            &principal.session_id,
                            &decoded,
                            t0,
                        );
                        if sticky_auth_context
                            && (sticky_principal.is_none() || sticky_token != decoded.jwt_token)
                        {
                            sticky_token.clear();
                            sticky_token.push_str(decoded.jwt_token);
                            sticky_principal = Some(principal.clone());
                        }
                        encode_v3_tcp_response(status, &body)
                    }
                    Err((status, reason_code)) => {
                        encode_v3_tcp_decode_error(status, reason_code, t0)
                    }
                }
            }
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

#[derive(Clone, Copy, Debug)]
enum V3DurableControlPreset {
    Legacy,
    HftStable,
}

impl V3DurableControlPreset {
    fn from_env() -> Self {
        match std::env::var("V3_DURABLE_CONTROL_PRESET")
            .unwrap_or_else(|_| "hft_stable".to_string())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "legacy" => Self::Legacy,
            "hft_stable" | "stable" | "hft" => Self::HftStable,
            _ => Self::HftStable,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::HftStable => "hft_stable",
        }
    }
}

#[derive(Clone, Copy)]
struct V3DurableWorkerPressureConfig {
    control_preset: V3DurableControlPreset,
    durable_slo_stage: u64,
    dynamic_inflight_enabled: bool,
    dynamic_inflight_min_cap_pct: u64,
    dynamic_inflight_max_cap_pct: u64,
    dynamic_inflight_strict_max_cap_pct: u64,
    early_soft_age_us: u64,
    early_hard_age_us: u64,
    age_soft_inflight_cap_pct: u64,
    age_hard_inflight_cap_pct: u64,
    fsync_soft_inflight_cap_pct: u64,
    fsync_hard_inflight_cap_pct: u64,
    fsync_soft_trigger_us: u64,
    fsync_hard_trigger_us: u64,
    pressure_ewma_alpha_pct: u64,
    dynamic_cap_slew_step_pct: u64,
}

impl V3DurableWorkerPressureConfig {
    fn from_env(inflight_hard_cap_pct: u64) -> Self {
        let control_preset = V3DurableControlPreset::from_env();
        let durable_slo_stage = std::env::var("V3_DURABLE_SLO_STAGE")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(1);
        let (stage_soft_age_us, stage_hard_age_us) =
            v3_durable_slo_age_targets(durable_slo_stage, control_preset);
        let defaults = match control_preset {
            V3DurableControlPreset::Legacy => (
                inflight_hard_cap_pct.max(1),
                100,
                100,
                50,
                20,
                100,
                100,
                100,
                100,
            ),
            // HFT-stable baseline: avoid cap oscillation by smoothing pressure and limiting slew.
            V3DurableControlPreset::HftStable => (10, 40, 40, 35, 15, 30, 8, 70, 35),
        };
        let (
            default_dynamic_min,
            default_dynamic_max,
            default_dynamic_strict_max,
            default_age_soft,
            default_age_hard,
            default_alpha,
            default_slew,
            default_fsync_soft_cap,
            default_fsync_hard_cap,
        ) = defaults;

        let dynamic_inflight_default = matches!(control_preset, V3DurableControlPreset::Legacy);
        let dynamic_inflight_enabled = parse_bool_env("V3_DURABLE_WORKER_DYNAMIC_INFLIGHT")
            .unwrap_or(dynamic_inflight_default);
        let dynamic_inflight_strict_max_cap_pct =
            std::env::var("V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_STRICT_MAX_CAP_PCT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default_dynamic_strict_max)
                .clamp(1, 100);
        let mut dynamic_inflight_min_cap_pct =
            std::env::var("V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MIN_CAP_PCT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default_dynamic_min)
                .clamp(1, 100);
        let dynamic_inflight_max_cap_pct =
            std::env::var("V3_DURABLE_WORKER_DYNAMIC_INFLIGHT_MAX_CAP_PCT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default_dynamic_max)
                .clamp(1, 100)
                .min(dynamic_inflight_strict_max_cap_pct);
        if dynamic_inflight_min_cap_pct > dynamic_inflight_max_cap_pct {
            dynamic_inflight_min_cap_pct = dynamic_inflight_max_cap_pct;
        }

        let early_soft_age_us = std::env::var("V3_DURABLE_SLO_EARLY_SOFT_AGE_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(stage_soft_age_us);
        let mut early_hard_age_us = std::env::var("V3_DURABLE_SLO_EARLY_HARD_AGE_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(stage_hard_age_us);
        if early_soft_age_us > 0 && early_hard_age_us > 0 && early_hard_age_us <= early_soft_age_us
        {
            early_hard_age_us = early_soft_age_us.saturating_add(1);
        }

        let age_soft_inflight_cap_pct = std::env::var("V3_DURABLE_AGE_SOFT_INFLIGHT_CAP_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_age_soft)
            .clamp(1, 100);
        let mut age_hard_inflight_cap_pct = std::env::var("V3_DURABLE_AGE_HARD_INFLIGHT_CAP_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_age_hard)
            .clamp(1, 100);
        if age_hard_inflight_cap_pct > age_soft_inflight_cap_pct {
            age_hard_inflight_cap_pct = age_soft_inflight_cap_pct;
        }
        let fsync_soft_inflight_cap_pct = std::env::var("V3_DURABLE_FSYNC_SOFT_INFLIGHT_CAP_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_fsync_soft_cap)
            .clamp(1, 100);
        let mut fsync_hard_inflight_cap_pct =
            std::env::var("V3_DURABLE_FSYNC_HARD_INFLIGHT_CAP_PCT")
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(default_fsync_hard_cap)
                .clamp(1, 100);
        if fsync_hard_inflight_cap_pct > fsync_soft_inflight_cap_pct {
            fsync_hard_inflight_cap_pct = fsync_soft_inflight_cap_pct;
        }
        let fsync_soft_trigger_us = std::env::var("V3_DURABLE_FSYNC_SOFT_TRIGGER_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_else(|| {
                std::env::var("V3_DURABLE_ADMISSION_SOFT_FSYNC_P99_US")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(6_000)
            });
        let mut fsync_hard_trigger_us = std::env::var("V3_DURABLE_FSYNC_HARD_TRIGGER_US")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or_else(|| {
                std::env::var("V3_DURABLE_ADMISSION_HARD_FSYNC_P99_US")
                    .ok()
                    .and_then(|v| v.parse::<u64>().ok())
                    .unwrap_or(12_000)
            })
            .max(fsync_soft_trigger_us);
        if fsync_soft_trigger_us > 0
            && fsync_hard_trigger_us > 0
            && fsync_hard_trigger_us <= fsync_soft_trigger_us
        {
            fsync_hard_trigger_us = fsync_soft_trigger_us.saturating_add(1);
        }

        let pressure_ewma_alpha_pct = std::env::var("V3_DURABLE_PRESSURE_EWMA_ALPHA_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_alpha)
            .clamp(1, 100);
        let dynamic_cap_slew_step_pct = std::env::var("V3_DURABLE_DYNAMIC_CAP_SLEW_STEP_PCT")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(default_slew)
            .clamp(1, 100);

        Self {
            control_preset,
            durable_slo_stage,
            dynamic_inflight_enabled,
            dynamic_inflight_min_cap_pct,
            dynamic_inflight_max_cap_pct,
            dynamic_inflight_strict_max_cap_pct,
            early_soft_age_us,
            early_hard_age_us,
            age_soft_inflight_cap_pct,
            age_hard_inflight_cap_pct,
            fsync_soft_inflight_cap_pct,
            fsync_hard_inflight_cap_pct,
            fsync_soft_trigger_us,
            fsync_hard_trigger_us,
            pressure_ewma_alpha_pct,
            dynamic_cap_slew_step_pct,
        }
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

fn v3_confirm_age_pressure_ratio(current_us: u64, soft_us: u64, hard_us: u64) -> f64 {
    if soft_us == 0 && hard_us == 0 {
        return 0.0;
    }
    if soft_us > 0 && hard_us > soft_us {
        if current_us <= soft_us {
            0.0
        } else if current_us >= hard_us {
            1.0
        } else {
            (current_us - soft_us) as f64 / (hard_us - soft_us) as f64
        }
    } else if soft_us > 0 {
        (current_us as f64 / soft_us as f64).clamp(0.0, 1.0)
    } else if hard_us > 0 {
        (current_us as f64 / hard_us as f64).clamp(0.0, 1.0)
    } else {
        0.0
    }
}

#[inline]
fn v3_pressure_ewma_u64(prev: u64, sample: u64, alpha_pct: u64) -> u64 {
    let alpha = alpha_pct.clamp(1, 100);
    if prev == 0 {
        return sample.min(100);
    }
    let keep = 100u128.saturating_sub(alpha as u128);
    (((prev as u128).saturating_mul(keep) + (sample as u128).saturating_mul(alpha as u128) + 50)
        / 100) as u64
}

#[inline]
fn v3_ewma_u64(prev: u64, sample: u64, alpha_pct: u64) -> u64 {
    let alpha = alpha_pct.clamp(1, 100);
    if prev == 0 {
        return sample;
    }
    let keep = 100u128.saturating_sub(alpha as u128);
    (((prev as u128).saturating_mul(keep) + (sample as u128).saturating_mul(alpha as u128) + 50)
        / 100) as u64
}

#[inline]
fn v3_durable_confirm_age_target_us(min_us: u64, max_us: u64, pressure_pct: u64) -> u64 {
    if min_us == 0 && max_us == 0 {
        return 0;
    }
    let lower = min_us.min(max_us);
    let upper = min_us.max(max_us);
    if lower == upper {
        return upper;
    }
    let pressure = pressure_pct.min(100) as u128;
    let span = (upper - lower) as u128;
    (upper as u128)
        .saturating_sub((span.saturating_mul(pressure) + 50) / 100)
        .clamp(lower as u128, upper as u128) as u64
}

#[inline]
fn v3_confirm_autotune_admission_floor_pct(admission_level: u64) -> u64 {
    match admission_level {
        0 => 0,
        1 => 70,
        _ => 90,
    }
}

#[inline]
fn v3_confirm_autotune_queue_pressure_pct(
    lane_pressure_pct: u64,
    age_pressure_pct: u64,
    admission_level: u64,
) -> u64 {
    // Queue pressure alone can over-tighten confirm-age under healthy age latency.
    // Use it only when confirm age has started rising or admission is already elevated.
    if age_pressure_pct > 0 || admission_level > 0 {
        lane_pressure_pct.min(100)
    } else {
        0
    }
}

#[inline]
fn apply_confirm_guard_slack(age_us: u64, slack_pct: u64) -> u64 {
    if age_us == 0 || slack_pct == 0 {
        age_us
    } else {
        age_us.saturating_add(((age_us as u128).saturating_mul(slack_pct as u128) / 100) as u64)
    }
}

#[inline]
fn v3_confirm_guard_effective_ticks(
    base_ticks: u64,
    pressure_pct: u64,
    low_pressure_pct: u64,
    high_pressure_pct: u64,
    tighten_delta: u64,
    relax_delta: u64,
) -> u64 {
    let base = base_ticks.max(1);
    if high_pressure_pct <= low_pressure_pct {
        return base;
    }
    if pressure_pct >= high_pressure_pct {
        base.saturating_sub(tighten_delta).max(1)
    } else if pressure_pct <= low_pressure_pct {
        base.saturating_add(relax_delta).max(1)
    } else {
        base
    }
}

#[inline]
fn v3_slew_toward_us(
    current_us: u64,
    target_us: u64,
    tighten_step_pct: u64,
    relax_step_pct: u64,
    min_step_us: u64,
) -> u64 {
    if current_us == 0 || target_us == 0 || current_us == target_us {
        return target_us;
    }
    let min_step = min_step_us.max(1);
    if target_us < current_us {
        let step = (((current_us as u128).saturating_mul(tighten_step_pct.max(1) as u128) + 99)
            / 100) as u64;
        current_us.saturating_sub(step.max(min_step)).max(target_us)
    } else {
        let step = (((current_us as u128).saturating_mul(relax_step_pct.max(1) as u128) + 99) / 100)
            as u64;
        current_us.saturating_add(step.max(min_step)).min(target_us)
    }
}

#[inline]
fn current_utc_hour_slot() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| ((d.as_secs() / 3_600) % 24) as usize)
        .unwrap_or(0)
}

fn v3_durable_slo_age_targets(stage: u64, preset: V3DurableControlPreset) -> (u64, u64) {
    match preset {
        V3DurableControlPreset::Legacy => match stage {
            0 => (0, 0),
            1 => (250_000, 750_000),
            2 => (150_000, 500_000),
            3 => (100_000, 300_000),
            4 => (50_000, 150_000),
            _ => (250_000, 750_000),
        },
        V3DurableControlPreset::HftStable => match stage {
            0 => (0, 0),
            1 => (100_000, 220_000),
            2 => (80_000, 180_000),
            3 => (60_000, 140_000),
            4 => (40_000, 100_000),
            _ => (100_000, 220_000),
        },
    }
}

fn choose_tighter_age_target(preferred: u64, fallback: u64) -> u64 {
    match (preferred, fallback) {
        (0, 0) => 0,
        (a, 0) => a,
        (0, b) => b,
        (a, b) => a.min(b),
    }
}

async fn resolve_audit_append_receipt(
    append: AuditAppendWithReceipt,
    timeout: Duration,
    failed_reason: &'static str,
    closed_reason: &'static str,
    timeout_reason: &'static str,
) -> (bool, u64, bool, &'static str) {
    if append.timings.durable_done_ns > 0 {
        return (true, append.timings.fdatasync_ns, false, "");
    }
    let Some(durable_rx) = append.durable_rx else {
        return (false, 0, false, failed_reason);
    };
    match tokio::time::timeout(timeout, durable_rx).await {
        Ok(Ok(receipt)) if receipt.durable_done_ns > 0 => (true, receipt.fdatasync_ns, false, ""),
        Ok(Ok(_)) => (false, 0, false, failed_reason),
        Ok(Err(_)) => (false, 0, false, closed_reason),
        Err(_) => (false, 0, true, timeout_reason),
    }
}

/// /v3/orders の single-writer worker。
/// ホットキューを直列消費し、durable confirm 経路へ渡す。
async fn run_v3_single_writer(shard_id: usize, mut rx: Receiver<V3OrderTask>, state: AppState) {
    while let Some(task) = rx.recv().await {
        state.v3_ingress.on_processed_one(shard_id);
        state.apply_v3_position_delta(
            Arc::clone(&task.account_id),
            task.position_symbol_key,
            task.position_delta_qty,
        );
        let confirm_lane_id = state.v3_durable_ingress.lane_for_shard(task.shard_id);
        state.v3_confirm_store.record_volatile_in_lane(
            confirm_lane_id,
            &task,
            gateway_core::now_nanos(),
        );
        match state
            .v3_durable_ingress
            .try_enqueue(task.shard_id, task.into())
        {
            Ok(()) => {}
            Err(TrySendError::Full(task)) => {
                state.register_v3_loss_suspect(
                    task.session_id.as_ref(),
                    task.session_seq,
                    shard_id,
                    "DURABILITY_QUEUE_FULL",
                    gateway_core::now_nanos(),
                );
            }
            Err(TrySendError::Closed(task)) => {
                state.register_v3_loss_suspect(
                    task.session_id.as_ref(),
                    task.session_seq,
                    shard_id,
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
    pressure_cfg: V3DurableWorkerPressureConfig,
) {
    #[derive(Clone, Copy)]
    struct DurableResolution {
        durable_done_ns: u64,
        fdatasync_ns: u64,
        reject_reason: &'static str,
        timed_out: bool,
    }

    let fallback_batch_max = batch_max.max(1);
    let fallback_batch_wait = Duration::from_micros(batch_wait_us.max(1));
    let lane_audit_log = state
        .v3_durable_audit_logs
        .get(lane_id)
        .cloned()
        .unwrap_or_else(|| Arc::clone(&state.audit_log));
    let replica_enabled = state.v3_durable_replica_enabled;
    let replica_required = state.v3_durable_replica_required;
    let replica_receipt_timeout =
        Duration::from_micros(state.v3_durable_replica_receipt_timeout_us.max(1));
    let replica_audit_log = if replica_enabled {
        let replica_path = v3_durable_lane_replica_wal_path(lane_audit_log.path());
        match AuditLog::new(&replica_path) {
            Ok(log) => {
                let log = Arc::new(log);
                log.clone().start_async_writer(None);
                Some(log)
            }
            Err(err) => {
                tracing::warn!(
                    lane_id = lane_id,
                    path = %replica_path.display(),
                    error = %err,
                    "failed to initialize durable replica wal; fallback to single wal"
                );
                None
            }
        }
    } else {
        None
    };
    let receipt_timeout = Duration::from_micros(state.v3_durable_worker_receipt_timeout_us.max(1));
    let max_inflight_receipts = state
        .v3_durable_worker_max_inflight_receipts
        .max(fallback_batch_max);
    let max_inflight_receipts_global = state.v3_durable_worker_max_inflight_receipts_global.max(1);
    let inflight_soft_cap_pct = state.v3_durable_worker_inflight_soft_cap_pct;
    let inflight_hard_cap_pct = state.v3_durable_worker_inflight_hard_cap_pct;
    let dynamic_inflight_enabled = pressure_cfg.dynamic_inflight_enabled;
    let dynamic_inflight_min_cap_pct = pressure_cfg.dynamic_inflight_min_cap_pct;
    let dynamic_inflight_max_cap_pct = pressure_cfg.dynamic_inflight_max_cap_pct;
    let early_soft_age_us = pressure_cfg.early_soft_age_us;
    let early_hard_age_us = pressure_cfg.early_hard_age_us;
    let age_soft_inflight_cap_pct = pressure_cfg.age_soft_inflight_cap_pct;
    let age_hard_inflight_cap_pct = pressure_cfg.age_hard_inflight_cap_pct;
    let fsync_soft_inflight_cap_pct = pressure_cfg.fsync_soft_inflight_cap_pct;
    let fsync_hard_inflight_cap_pct = pressure_cfg.fsync_hard_inflight_cap_pct;
    let fsync_soft_trigger_us = pressure_cfg.fsync_soft_trigger_us;
    let fsync_hard_trigger_us = pressure_cfg.fsync_hard_trigger_us;
    let pressure_alpha = (pressure_cfg.pressure_ewma_alpha_pct as f64 / 100.0).clamp(0.01, 1.0);
    let cap_slew_step_pct = pressure_cfg.dynamic_cap_slew_step_pct.max(1);
    if lane_id == 0 {
        info!(
            preset = pressure_cfg.control_preset.as_str(),
            stage = pressure_cfg.durable_slo_stage,
            dynamic_inflight_enabled = dynamic_inflight_enabled,
            dynamic_min_cap_pct = dynamic_inflight_min_cap_pct,
            dynamic_max_cap_pct = dynamic_inflight_max_cap_pct,
            dynamic_strict_max_cap_pct = pressure_cfg.dynamic_inflight_strict_max_cap_pct,
            early_soft_age_us = early_soft_age_us,
            early_hard_age_us = early_hard_age_us,
            age_soft_inflight_cap_pct = age_soft_inflight_cap_pct,
            age_hard_inflight_cap_pct = age_hard_inflight_cap_pct,
            fsync_soft_inflight_cap_pct = fsync_soft_inflight_cap_pct,
            fsync_hard_inflight_cap_pct = fsync_hard_inflight_cap_pct,
            fsync_soft_trigger_us = fsync_soft_trigger_us,
            fsync_hard_trigger_us = fsync_hard_trigger_us,
            pressure_ewma_alpha_pct = pressure_cfg.pressure_ewma_alpha_pct,
            dynamic_cap_slew_step_pct = cap_slew_step_pct,
            "v3 durable worker pressure config"
        );
    }
    let mut ingress_closed = false;
    use futures::{FutureExt, StreamExt, future::BoxFuture};
    let mut inflight: futures::stream::FuturesUnordered<
        BoxFuture<'static, (V3DurableTask, DurableResolution)>,
    > = futures::stream::FuturesUnordered::new();
    let mut smoothed_pressure_pct = 0.0f64;
    let mut pressure_initialized = false;
    let mut prev_dynamic_cap_pct = dynamic_inflight_max_cap_pct;

    #[inline]
    fn push_json_escaped_fragment(out: &mut Vec<u8>, value: &str) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        for &b in value.as_bytes() {
            match b {
                b'"' => out.extend_from_slice(br#"\""#),
                b'\\' => out.extend_from_slice(br#"\\"#),
                b'\n' => out.extend_from_slice(br#"\n"#),
                b'\r' => out.extend_from_slice(br#"\r"#),
                b'\t' => out.extend_from_slice(br#"\t"#),
                b'\x08' => out.extend_from_slice(br#"\b"#),
                b'\x0c' => out.extend_from_slice(br#"\f"#),
                0x00..=0x1f => {
                    out.extend_from_slice(br#"\u00"#);
                    out.push(HEX[(b >> 4) as usize]);
                    out.push(HEX[(b & 0x0f) as usize]);
                }
                _ => out.push(b),
            }
        }
    }

    #[inline]
    fn push_json_string(out: &mut Vec<u8>, value: &str) {
        out.push(b'"');
        push_json_escaped_fragment(out, value);
        out.push(b'"');
    }

    #[inline]
    fn push_u64_decimal(out: &mut Vec<u8>, mut value: u64) {
        if value == 0 {
            out.push(b'0');
            return;
        }
        let mut buf = [0u8; 20];
        let mut pos = buf.len();
        while value > 0 {
            pos = pos.saturating_sub(1);
            buf[pos] = b'0' + (value % 10) as u8;
            value /= 10;
        }
        out.extend_from_slice(&buf[pos..]);
    }

    #[inline]
    fn push_i64_decimal(out: &mut Vec<u8>, value: i64) {
        if value < 0 {
            out.push(b'-');
            let abs = (-(value as i128)) as u64;
            push_u64_decimal(out, abs);
            return;
        }
        push_u64_decimal(out, value as u64);
    }

    #[inline]
    fn build_v3_durable_accepted_line(task: &V3DurableTask, event_at_ms: u64) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(160 + task.session_id.len() + task.account_id.len().saturating_mul(2));
        let symbol_key = u64::from_le_bytes(task.position_symbol_key);
        out.extend_from_slice(b"{\"type\":\"V3DurableAccepted\",\"at\":");
        push_u64_decimal(&mut out, event_at_ms);
        out.extend_from_slice(b",\"accountId\":");
        push_json_string(&mut out, task.account_id.as_ref());
        out.extend_from_slice(b",\"orderId\":");
        out.push(b'"');
        out.extend_from_slice(b"v3/");
        push_json_escaped_fragment(&mut out, task.session_id.as_ref());
        out.push(b'/');
        push_u64_decimal(&mut out, task.session_seq);
        out.push(b'"');
        out.extend_from_slice(b",\"data\":{\"sessionSeq\":");
        push_u64_decimal(&mut out, task.session_seq);
        out.extend_from_slice(b",\"attemptId\":\"att_");
        push_u64_decimal(&mut out, task.attempt_seq);
        out.extend_from_slice(b"\",\"positionSymbolKey\":");
        push_u64_decimal(&mut out, symbol_key);
        out.extend_from_slice(b",\"positionDeltaQty\":");
        push_i64_decimal(&mut out, task.position_delta_qty);
        out.extend_from_slice(b",\"shardId\":");
        push_u64_decimal(&mut out, task.shard_id as u64);
        out.extend_from_slice(b"}}");
        out.push(b'\n');
        out
    }

    let apply_outcome = |task: V3DurableTask, outcome: DurableResolution| {
        if outcome.timed_out {
            state
                .v3_durable_receipt_timeout_total
                .fetch_add(1, Ordering::Relaxed);
        }
        if outcome.fdatasync_ns > 0 {
            state
                .v3_durable_wal_fsync_hist
                .record(outcome.fdatasync_ns / 1_000);
            if let Some(hist) = state.v3_durable_wal_fsync_hist_per_lane.get(lane_id) {
                hist.record(outcome.fdatasync_ns / 1_000);
            }
        }

        let now_ns = gateway_core::now_nanos();
        let V3DurableTask {
            session_id,
            session_seq,
            received_at_ns,
            ..
        } = task;
        if outcome.durable_done_ns > 0 {
            state.v3_confirm_store.mark_durable_accepted_in_lane(
                lane_id,
                session_id.as_ref(),
                session_seq,
                now_ns,
            );
            state.increment_v3_durable_accepted_total(lane_id);
        } else {
            state.v3_confirm_store.mark_durable_rejected_in_lane(
                lane_id,
                session_id.as_ref(),
                session_seq,
                outcome.reject_reason,
                now_ns,
            );
            state.increment_v3_durable_rejected_total(lane_id);
            state
                .v3_durable_write_error_total
                .fetch_add(1, Ordering::Relaxed);
        }
        let elapsed_us = now_ns.saturating_sub(received_at_ns) / 1_000;
        state.v3_durable_confirm_hist.record(elapsed_us);
    };
    let refresh_inflight_metrics = |lane_inflight: usize| -> usize {
        if let Some(gauge) = state.v3_durable_receipt_inflight_per_lane.get(lane_id) {
            gauge.store(lane_inflight as u64, Ordering::Relaxed);
        }
        if let Some(gauge) = state.v3_durable_receipt_inflight_max_per_lane.get(lane_id) {
            gauge.fetch_max(lane_inflight as u64, Ordering::Relaxed);
        }
        let total_inflight = state
            .v3_durable_receipt_inflight_per_lane
            .iter()
            .map(|v| v.load(Ordering::Relaxed))
            .sum::<u64>() as usize;
        state
            .v3_durable_receipt_inflight
            .store(total_inflight as u64, Ordering::Relaxed);
        state
            .v3_durable_receipt_inflight_max
            .fetch_max(total_inflight as u64, Ordering::Relaxed);
        total_inflight
    };

    loop {
        let worker_loop_t0 = gateway_core::now_nanos();
        let mut progressed = false;

        let mut drained = 0usize;
        let drain_budget = inflight
            .len()
            .max(fallback_batch_max.saturating_mul(8))
            .clamp(64, 4096);
        while drained < drain_budget {
            match inflight.next().now_or_never() {
                Some(Some((task, outcome))) => {
                    apply_outcome(task, outcome);
                    drained += 1;
                    progressed = true;
                }
                Some(None) | None => break,
            }
        }

        let total_inflight = refresh_inflight_metrics(inflight.len());
        let lane_util_pct = state.v3_durable_ingress.lane_utilization_pct(lane_id);
        let lane_backlog_growth_per_sec = state
            .v3_durable_backlog_growth_per_sec_per_lane
            .get(lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0);
        let lane_fsync_p99_us = state
            .v3_durable_fsync_p99_cached_us_per_lane
            .get(lane_id)
            .map(|value| value.load(Ordering::Relaxed))
            .unwrap_or_else(|| state.v3_durable_fsync_p99_cached_us.load(Ordering::Relaxed));

        let lane_level = state
            .v3_durable_admission_level_per_lane
            .get(lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or(0);
        let inflight_cap_pct = match lane_level {
            2 => inflight_hard_cap_pct,
            1 => inflight_soft_cap_pct,
            _ => 100,
        };
        let effective_max_inflight = (((max_inflight_receipts as u128)
            .saturating_mul(inflight_cap_pct as u128)
            / 100) as usize)
            .max(fallback_batch_max)
            .min(max_inflight_receipts);
        let confirm_oldest_age_us_lane = state
            .v3_confirm_oldest_inflight_us_per_lane
            .get(lane_id)
            .map(|v| v.load(Ordering::Relaxed))
            .unwrap_or_else(|| state.v3_confirm_oldest_inflight_us.load(Ordering::Relaxed));
        let (confirm_soft_age_us, confirm_hard_age_us) =
            state.v3_durable_confirm_reject_ages_for_lane(lane_id);
        let soft_pressure_age_us = choose_tighter_age_target(
            early_soft_age_us,
            if confirm_soft_age_us > 0 {
                confirm_soft_age_us.saturating_mul(8) / 10
            } else {
                0
            },
        );
        let hard_pressure_age_us = choose_tighter_age_target(
            early_hard_age_us,
            if confirm_hard_age_us > 0 {
                confirm_hard_age_us.saturating_mul(9) / 10
            } else {
                0
            },
        );
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
        let confirm_ratio = v3_confirm_age_pressure_ratio(
            confirm_oldest_age_us_lane,
            soft_pressure_age_us,
            hard_pressure_age_us,
        );
        let lane_pressure_pct_raw = ((util_ratio * 0.45
            + backlog_ratio * 0.25
            + fsync_ratio * 0.15
            + confirm_ratio * 0.15)
            * 100.0)
            .clamp(0.0, 100.0);
        if !pressure_initialized {
            smoothed_pressure_pct = lane_pressure_pct_raw;
            pressure_initialized = true;
        } else {
            smoothed_pressure_pct = (pressure_alpha * lane_pressure_pct_raw)
                + ((1.0 - pressure_alpha) * smoothed_pressure_pct);
        }
        let lane_pressure_pct = smoothed_pressure_pct.clamp(0.0, 100.0);
        if let Some(gauge) = state.v3_durable_pressure_pct_per_lane.get(lane_id) {
            gauge.store(lane_pressure_pct.round() as u64, Ordering::Relaxed);
        }
        let mut effective_max_inflight = effective_max_inflight;
        if soft_pressure_age_us > 0 {
            if confirm_oldest_age_us_lane >= soft_pressure_age_us {
                let soft_cap = (((max_inflight_receipts as u128)
                    .saturating_mul(age_soft_inflight_cap_pct as u128)
                    / 100) as usize)
                    .max(fallback_batch_max)
                    .min(max_inflight_receipts);
                effective_max_inflight = effective_max_inflight.min(soft_cap);
            }
        }
        if hard_pressure_age_us > 0 {
            if confirm_oldest_age_us_lane >= hard_pressure_age_us {
                let hard_cap = (((max_inflight_receipts as u128)
                    .saturating_mul(age_hard_inflight_cap_pct as u128)
                    / 100) as usize)
                    .max(fallback_batch_max)
                    .min(max_inflight_receipts);
                effective_max_inflight = effective_max_inflight.min(hard_cap);
            }
        }
        if fsync_soft_trigger_us > 0 && lane_fsync_p99_us >= fsync_soft_trigger_us {
            let fsync_soft_cap = (((max_inflight_receipts as u128)
                .saturating_mul(fsync_soft_inflight_cap_pct as u128)
                / 100) as usize)
                .max(fallback_batch_max)
                .min(max_inflight_receipts);
            effective_max_inflight = effective_max_inflight.min(fsync_soft_cap);
        }
        if fsync_hard_trigger_us > 0 && lane_fsync_p99_us >= fsync_hard_trigger_us {
            let fsync_hard_cap = (((max_inflight_receipts as u128)
                .saturating_mul(fsync_hard_inflight_cap_pct as u128)
                / 100) as usize)
                .max(fallback_batch_max)
                .min(max_inflight_receipts);
            effective_max_inflight = effective_max_inflight.min(fsync_hard_cap);
        }
        let mut dynamic_cap_pct_applied = 100u64;
        if dynamic_inflight_enabled {
            let dynamic_range =
                dynamic_inflight_max_cap_pct.saturating_sub(dynamic_inflight_min_cap_pct);
            let dynamic_step =
                ((dynamic_range as f64) * (lane_pressure_pct / 100.0)).round() as u64;
            let target_dynamic_cap_pct = dynamic_inflight_max_cap_pct
                .saturating_sub(dynamic_step)
                .max(dynamic_inflight_min_cap_pct);
            let mut dynamic_cap_pct = target_dynamic_cap_pct;
            if cap_slew_step_pct < 100 {
                let lower = prev_dynamic_cap_pct.saturating_sub(cap_slew_step_pct);
                let upper = prev_dynamic_cap_pct
                    .saturating_add(cap_slew_step_pct)
                    .min(100);
                dynamic_cap_pct = dynamic_cap_pct.clamp(lower, upper);
            }
            dynamic_cap_pct =
                dynamic_cap_pct.clamp(dynamic_inflight_min_cap_pct, dynamic_inflight_max_cap_pct);
            prev_dynamic_cap_pct = dynamic_cap_pct;
            dynamic_cap_pct_applied = dynamic_cap_pct;
            let dynamic_cap = (((max_inflight_receipts as u128)
                .saturating_mul(dynamic_cap_pct as u128)
                / 100) as usize)
                .max(fallback_batch_max)
                .min(max_inflight_receipts);
            effective_max_inflight = effective_max_inflight.min(dynamic_cap);
        } else {
            prev_dynamic_cap_pct = 100;
        }
        if let Some(gauge) = state.v3_durable_dynamic_cap_pct_per_lane.get(lane_id) {
            gauge.store(dynamic_cap_pct_applied, Ordering::Relaxed);
        }

        if inflight.len() >= effective_max_inflight
            || total_inflight >= max_inflight_receipts_global
        {
            if let Some((task, outcome)) = inflight.next().await {
                apply_outcome(task, outcome);
                progressed = true;
            } else if ingress_closed {
                break;
            }
        } else {
            let mut first = None;
            if !ingress_closed {
                if progressed {
                    match rx.try_recv() {
                        Ok(task) => first = Some(task),
                        Err(TryRecvError::Empty) => {}
                        Err(TryRecvError::Disconnected) => {
                            ingress_closed = true;
                        }
                    }
                } else if !inflight.is_empty() {
                    tokio::select! {
                        maybe_done = inflight.next() => {
                            if let Some((task, outcome)) = maybe_done {
                                apply_outcome(task, outcome);
                                progressed = true;
                            }
                        }
                        maybe_task = rx.recv() => {
                            match maybe_task {
                                Some(task) => {
                                    first = Some(task);
                                    progressed = true;
                                }
                                None => {
                                    ingress_closed = true;
                                }
                            }
                        }
                    }
                } else {
                    match rx.recv().await {
                        Some(task) => {
                            first = Some(task);
                            progressed = true;
                        }
                        None => {
                            ingress_closed = true;
                        }
                    }
                }
            }

            if let Some(first) = first {
                let (target_batch_max, target_batch_wait) = if batch_adaptive_cfg.enabled {
                    batch_adaptive_cfg.target_for_pressure(lane_pressure_pct)
                } else {
                    (fallback_batch_max, fallback_batch_wait)
                };
                // Confirm age approaches soft/hard guard: prioritize latency over batch efficiency.
                let mut target_batch_max = target_batch_max;
                let mut target_batch_wait = target_batch_wait;
                if soft_pressure_age_us > 0 && confirm_oldest_age_us_lane >= soft_pressure_age_us {
                    target_batch_wait = target_batch_wait.min(batch_adaptive_cfg.wait_min);
                    target_batch_max = target_batch_max.min((fallback_batch_max / 2).max(1));
                }
                if hard_pressure_age_us > 0 && confirm_oldest_age_us_lane >= hard_pressure_age_us {
                    target_batch_wait = Duration::from_micros(1);
                    target_batch_max = 1;
                }
                let lane_headroom = effective_max_inflight.saturating_sub(inflight.len());
                let global_headroom = max_inflight_receipts_global.saturating_sub(total_inflight);
                let target_batch_max = target_batch_max
                    .min(lane_headroom.max(1))
                    .min(global_headroom.max(1));
                let mut batch = Vec::with_capacity(target_batch_max.max(1));
                batch.push(first);
                if target_batch_max > 1 {
                    let deadline = tokio::time::Instant::now() + target_batch_wait;
                    while batch.len() < target_batch_max {
                        match tokio::time::timeout_at(deadline, rx.recv()).await {
                            Ok(Some(task)) => batch.push(task),
                            Ok(None) => {
                                ingress_closed = true;
                                break;
                            }
                            Err(_) => break,
                        }
                    }
                }

                for task in batch {
                    state.v3_durable_ingress.on_processed_one(lane_id);
                    let append_t0 = gateway_core::now_nanos();
                    let event_line =
                        build_v3_durable_accepted_line(&task, crate::audit::now_millis());
                    if !replica_enabled {
                        // Fast-path for default mode: primary WAL only, no replica branches/clones.
                        let primary_append = lane_audit_log
                            .append_json_line_with_durable_receipt(event_line, append_t0);
                        let append_elapsed_us =
                            if primary_append.timings.enqueue_done_ns >= append_t0 {
                                (primary_append.timings.enqueue_done_ns - append_t0) / 1_000
                            } else {
                                gateway_core::now_nanos().saturating_sub(append_t0) / 1_000
                            };
                        state.v3_durable_wal_append_hist.record(append_elapsed_us);

                        if primary_append.timings.durable_done_ns > 0 {
                            apply_outcome(
                                task,
                                DurableResolution {
                                    durable_done_ns: primary_append.timings.durable_done_ns,
                                    fdatasync_ns: primary_append.timings.fdatasync_ns,
                                    reject_reason: "",
                                    timed_out: false,
                                },
                            );
                            continue;
                        }

                        if primary_append.durable_rx.is_none() {
                            apply_outcome(
                                task,
                                DurableResolution {
                                    durable_done_ns: 0,
                                    fdatasync_ns: 0,
                                    reject_reason: "WAL_DURABILITY_ENQUEUE_FAILED",
                                    timed_out: false,
                                },
                            );
                            continue;
                        }

                        inflight.push(
                            async move {
                                let (
                                    primary_ok,
                                    primary_fsync_ns,
                                    primary_timed_out,
                                    primary_reason,
                                ) = resolve_audit_append_receipt(
                                    primary_append,
                                    receipt_timeout,
                                    "WAL_DURABILITY_FAILED",
                                    "WAL_DURABILITY_RECEIPT_CLOSED",
                                    "WAL_DURABILITY_RECEIPT_TIMEOUT",
                                )
                                .await;
                                let outcome = if !primary_ok {
                                    DurableResolution {
                                        durable_done_ns: 0,
                                        fdatasync_ns: 0,
                                        reject_reason: primary_reason,
                                        timed_out: primary_timed_out,
                                    }
                                } else {
                                    DurableResolution {
                                        durable_done_ns: gateway_core::now_nanos(),
                                        fdatasync_ns: primary_fsync_ns,
                                        reject_reason: "",
                                        timed_out: false,
                                    }
                                };
                                (task, outcome)
                            }
                            .boxed(),
                        );
                        continue;
                    }

                    let primary_append = lane_audit_log
                        .append_json_line_with_durable_receipt(event_line.clone(), append_t0);
                    let append_elapsed_us = if primary_append.timings.enqueue_done_ns >= append_t0 {
                        (primary_append.timings.enqueue_done_ns - append_t0) / 1_000
                    } else {
                        gateway_core::now_nanos().saturating_sub(append_t0) / 1_000
                    };
                    state.v3_durable_wal_append_hist.record(append_elapsed_us);

                    if replica_required {
                        let replica_required_append = replica_audit_log.as_ref().map(|replica| {
                            state
                                .v3_durable_replica_append_total
                                .fetch_add(1, Ordering::Relaxed);
                            replica.append_json_line_with_durable_receipt(event_line, append_t0)
                        });
                        let state_for_receipt = state.clone();
                        inflight.push(
                            async move {
                                let (
                                    primary_ok,
                                    primary_fsync_ns,
                                    primary_timed_out,
                                    primary_reason,
                                ) = resolve_audit_append_receipt(
                                    primary_append,
                                    receipt_timeout,
                                    "WAL_DURABILITY_FAILED",
                                    "WAL_DURABILITY_RECEIPT_CLOSED",
                                    "WAL_DURABILITY_RECEIPT_TIMEOUT",
                                )
                                .await;
                                let (
                                    replica_ok,
                                    replica_fsync_ns,
                                    replica_timed_out,
                                    replica_reason,
                                ) = if let Some(replica_append) = replica_required_append {
                                    resolve_audit_append_receipt(
                                        replica_append,
                                        replica_receipt_timeout,
                                        "WAL_REPLICA_DURABILITY_FAILED",
                                        "WAL_REPLICA_DURABILITY_RECEIPT_CLOSED",
                                        "WAL_REPLICA_DURABILITY_RECEIPT_TIMEOUT",
                                    )
                                    .await
                                } else {
                                    (false, 0, false, "WAL_REPLICA_UNAVAILABLE")
                                };
                                let outcome = if !primary_ok {
                                    DurableResolution {
                                        durable_done_ns: 0,
                                        fdatasync_ns: 0,
                                        reject_reason: primary_reason,
                                        timed_out: primary_timed_out,
                                    }
                                } else if !replica_ok {
                                    DurableResolution {
                                        durable_done_ns: 0,
                                        fdatasync_ns: primary_fsync_ns,
                                        reject_reason: replica_reason,
                                        timed_out: replica_timed_out,
                                    }
                                } else {
                                    DurableResolution {
                                        durable_done_ns: gateway_core::now_nanos(),
                                        fdatasync_ns: primary_fsync_ns.max(replica_fsync_ns),
                                        reject_reason: "",
                                        timed_out: false,
                                    }
                                };
                                if !replica_ok {
                                    state_for_receipt
                                        .v3_durable_replica_write_error_total
                                        .fetch_add(1, Ordering::Relaxed);
                                    if replica_timed_out {
                                        state_for_receipt
                                            .v3_durable_replica_receipt_timeout_total
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                if outcome.reject_reason == "WAL_REPLICA_UNAVAILABLE" {
                                    state_for_receipt
                                        .v3_durable_write_error_total
                                        .fetch_add(1, Ordering::Relaxed);
                                }
                                (task, outcome)
                            }
                            .boxed(),
                        );
                        continue;
                    }

                    let replica_best_effort_timings = replica_audit_log.as_ref().map(|replica| {
                        state
                            .v3_durable_replica_append_total
                            .fetch_add(1, Ordering::Relaxed);
                        replica.append_json_line_with_timings(event_line, append_t0)
                    });
                    if let Some(replica_timings) = replica_best_effort_timings {
                        if replica_timings.enqueue_done_ns == 0
                            && replica_timings.durable_done_ns == 0
                        {
                            state
                                .v3_durable_replica_write_error_total
                                .fetch_add(1, Ordering::Relaxed);
                            state
                                .v3_durable_write_error_total
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if primary_append.timings.durable_done_ns > 0 {
                        apply_outcome(
                            task,
                            DurableResolution {
                                durable_done_ns: primary_append.timings.durable_done_ns,
                                fdatasync_ns: primary_append.timings.fdatasync_ns,
                                reject_reason: "",
                                timed_out: false,
                            },
                        );
                        continue;
                    }
                    if primary_append.durable_rx.is_none() {
                        apply_outcome(
                            task,
                            DurableResolution {
                                durable_done_ns: 0,
                                fdatasync_ns: 0,
                                reject_reason: "WAL_DURABILITY_ENQUEUE_FAILED",
                                timed_out: false,
                            },
                        );
                        continue;
                    }
                    inflight.push(
                        async move {
                            let (primary_ok, primary_fsync_ns, primary_timed_out, primary_reason) =
                                resolve_audit_append_receipt(
                                    primary_append,
                                    receipt_timeout,
                                    "WAL_DURABILITY_FAILED",
                                    "WAL_DURABILITY_RECEIPT_CLOSED",
                                    "WAL_DURABILITY_RECEIPT_TIMEOUT",
                                )
                                .await;
                            let outcome = if !primary_ok {
                                DurableResolution {
                                    durable_done_ns: 0,
                                    fdatasync_ns: 0,
                                    reject_reason: primary_reason,
                                    timed_out: primary_timed_out,
                                }
                            } else {
                                DurableResolution {
                                    durable_done_ns: gateway_core::now_nanos(),
                                    fdatasync_ns: primary_fsync_ns,
                                    reject_reason: "",
                                    timed_out: false,
                                }
                            };
                            (task, outcome)
                        }
                        .boxed(),
                    );
                }
            }
        }

        let _ = refresh_inflight_metrics(inflight.len());

        if ingress_closed && inflight.is_empty() {
            break;
        }

        let worker_loop_elapsed_us =
            gateway_core::now_nanos().saturating_sub(worker_loop_t0) / 1_000;
        state
            .v3_durable_worker_loop_hist
            .record(worker_loop_elapsed_us);
        if let Some(hist) = state.v3_durable_worker_loop_hist_per_lane.get(lane_id) {
            hist.record(worker_loop_elapsed_us);
        }

        if !progressed {
            tokio::task::yield_now().await;
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
    let mut fsync_soft_streak_per_lane = vec![0u64; lane_count];
    let mut fsync_hard_streak_per_lane = vec![0u64; lane_count];
    let mut confirm_soft_over_streak_per_lane = vec![0u64; lane_count];
    let mut confirm_hard_over_streak_per_lane = vec![0u64; lane_count];
    let mut confirm_soft_clear_streak_per_lane = vec![0u64; lane_count];
    let mut confirm_hard_clear_streak_per_lane = vec![0u64; lane_count];
    // Fsync-only escalation is opt-in.
    // Keep durability signals observable, but avoid hard-clamping admission by storage jitter
    // unless an explicit sustain tick is configured.
    let fsync_only_soft_sustain_ticks =
        std::env::var("V3_DURABLE_ADMISSION_FSYNC_ONLY_SOFT_SUSTAIN_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
    let fsync_only_hard_sustain_ticks =
        std::env::var("V3_DURABLE_ADMISSION_FSYNC_ONLY_HARD_SUSTAIN_TICKS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
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
        let fsync_ewma_alpha_pct = state.v3_durable_fsync_ewma_alpha_pct;
        let fsync_p99_global_sample = state.v3_durable_wal_fsync_hist.snapshot().percentile(99.0);
        let fsync_p99_global_prev = state.v3_durable_fsync_p99_cached_us.load(Ordering::Relaxed);
        let fsync_p99_global = v3_ewma_u64(
            fsync_p99_global_prev,
            fsync_p99_global_sample,
            fsync_ewma_alpha_pct,
        );
        state
            .v3_durable_fsync_p99_cached_us
            .store(fsync_p99_global, Ordering::Relaxed);
        let mut fsync_p99_per_lane = Vec::with_capacity(lane_count);
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
            let lane_fsync_p99_sample_us = state
                .v3_durable_wal_fsync_hist_per_lane
                .get(lane_id)
                .map(|hist| hist.snapshot().percentile(99.0))
                .unwrap_or(fsync_p99_global);
            let lane_fsync_p99_prev_us = state
                .v3_durable_fsync_p99_cached_us_per_lane
                .get(lane_id)
                .map(|cached| cached.load(Ordering::Relaxed))
                .unwrap_or(fsync_p99_global_prev);
            let lane_fsync_p99_us = v3_ewma_u64(
                lane_fsync_p99_prev_us,
                lane_fsync_p99_sample_us,
                fsync_ewma_alpha_pct,
            );
            fsync_p99_per_lane.push(lane_fsync_p99_us);
            if let Some(cached) = state.v3_durable_fsync_p99_cached_us_per_lane.get(lane_id) {
                cached.store(lane_fsync_p99_us, Ordering::Relaxed);
            }
        }
        let hour_slot = current_utc_hour_slot().min(23);
        if state.v3_durable_confirm_age_autotune_enabled {
            let alpha_pct = state.v3_durable_confirm_age_autotune_alpha_pct;
            const CONFIRM_AUTOTUNE_TIGHTEN_STEP_PCT: u64 = 20;
            const CONFIRM_AUTOTUNE_RELAX_STEP_PCT: u64 = 2;
            const CONFIRM_AUTOTUNE_MIN_STEP_US: u64 = 1_000;
            for lane_id in 0..lane_count {
                let lane_pressure_pct = state
                    .v3_durable_pressure_pct_per_lane
                    .get(lane_id)
                    .map(|v| v.load(Ordering::Relaxed))
                    .unwrap_or(0)
                    .max(
                        state
                            .v3_durable_ingress
                            .lane_utilization_pct(lane_id)
                            .round() as u64,
                    )
                    .min(100);
                let lane_confirm_age_us = confirm_oldest_age_us_per_lane
                    .get(lane_id)
                    .copied()
                    .unwrap_or(confirm_oldest_age_us_max);
                let base_soft_age_us = state.v3_durable_confirm_soft_reject_age_us;
                let base_hard_age_us = state.v3_durable_confirm_hard_reject_age_us;
                let age_pressure_pct = (v3_confirm_age_pressure_ratio(
                    lane_confirm_age_us,
                    base_soft_age_us,
                    base_hard_age_us,
                ) * 100.0)
                    .round() as u64;
                let lane_admission_level = state
                    .v3_durable_admission_level_per_lane
                    .get(lane_id)
                    .map(|v| v.load(Ordering::Relaxed))
                    .unwrap_or(0);
                let queue_pressure_pct = v3_confirm_autotune_queue_pressure_pct(
                    lane_pressure_pct,
                    age_pressure_pct,
                    lane_admission_level,
                );
                let admission_floor_pct =
                    v3_confirm_autotune_admission_floor_pct(lane_admission_level);
                let raw_pressure_pct = queue_pressure_pct
                    .max(age_pressure_pct)
                    .max(admission_floor_pct)
                    .min(100);
                let hourly_index = lane_id.saturating_mul(24).saturating_add(hour_slot);
                let mut hourly_ewma_pct = raw_pressure_pct;
                if let Some(hourly_slot) = state
                    .v3_durable_confirm_hourly_pressure_ewma_per_lane
                    .get(hourly_index)
                {
                    let prev = hourly_slot.load(Ordering::Relaxed);
                    hourly_ewma_pct = v3_pressure_ewma_u64(prev, raw_pressure_pct, alpha_pct);
                    hourly_slot.store(hourly_ewma_pct, Ordering::Relaxed);
                }
                let blended_pressure_pct = (((raw_pressure_pct as u128).saturating_mul(70)
                    + (hourly_ewma_pct as u128).saturating_mul(30)
                    + 50)
                    / 100) as u64;

                let mut tuned_soft_age_us = if state.v3_durable_confirm_soft_reject_age_us > 0 {
                    v3_durable_confirm_age_target_us(
                        state.v3_durable_confirm_soft_reject_age_min_us,
                        state.v3_durable_confirm_soft_reject_age_max_us,
                        blended_pressure_pct,
                    )
                } else {
                    0
                };
                let mut tuned_hard_age_us = if state.v3_durable_confirm_hard_reject_age_us > 0 {
                    v3_durable_confirm_age_target_us(
                        state.v3_durable_confirm_hard_reject_age_min_us,
                        state.v3_durable_confirm_hard_reject_age_max_us,
                        blended_pressure_pct,
                    )
                } else {
                    0
                };
                if base_soft_age_us > 0 {
                    tuned_soft_age_us = tuned_soft_age_us.min(base_soft_age_us);
                }
                if base_hard_age_us > 0 {
                    tuned_hard_age_us = tuned_hard_age_us.min(base_hard_age_us);
                }
                if state.v3_durable_confirm_age_fsync_linked
                    && state.v3_durable_confirm_age_fsync_max_relax_pct > 0
                {
                    let lane_fsync_p99_us = fsync_p99_per_lane
                        .get(lane_id)
                        .copied()
                        .unwrap_or(fsync_p99_global);
                    let fsync_relax_ratio = v3_fsync_pressure_ratio(
                        lane_fsync_p99_us,
                        state.v3_durable_confirm_age_fsync_soft_ref_us,
                        state.v3_durable_confirm_age_fsync_hard_ref_us,
                    );
                    let relax_pct = ((state.v3_durable_confirm_age_fsync_max_relax_pct as f64)
                        * fsync_relax_ratio)
                        .round() as u64;
                    if relax_pct > 0 {
                        if tuned_soft_age_us > 0 {
                            let soft_relaxed = ((tuned_soft_age_us as u128)
                                .saturating_mul((100 + relax_pct) as u128)
                                / 100) as u64;
                            let soft_relax_cap = if base_soft_age_us > 0 {
                                ((base_soft_age_us as u128).saturating_mul(
                                    (100 + state.v3_durable_confirm_age_fsync_max_relax_pct)
                                        as u128,
                                ) / 100) as u64
                            } else {
                                soft_relaxed
                            };
                            tuned_soft_age_us = soft_relaxed
                                .max(tuned_soft_age_us)
                                .min(soft_relax_cap.max(tuned_soft_age_us));
                        }
                        if tuned_hard_age_us > 0 {
                            let hard_relaxed = ((tuned_hard_age_us as u128)
                                .saturating_mul((100 + relax_pct) as u128)
                                / 100) as u64;
                            let hard_relax_cap = if base_hard_age_us > 0 {
                                ((base_hard_age_us as u128).saturating_mul(
                                    (100 + state.v3_durable_confirm_age_fsync_max_relax_pct)
                                        as u128,
                                ) / 100) as u64
                            } else {
                                hard_relaxed
                            };
                            tuned_hard_age_us = hard_relaxed
                                .max(tuned_hard_age_us)
                                .min(hard_relax_cap.max(tuned_hard_age_us));
                        }
                    }
                }
                if tuned_soft_age_us > 0
                    && tuned_hard_age_us > 0
                    && tuned_hard_age_us <= tuned_soft_age_us
                {
                    tuned_hard_age_us = tuned_soft_age_us.saturating_add(1);
                }
                if tuned_soft_age_us == 0 {
                    tuned_soft_age_us = state.v3_durable_confirm_soft_reject_age_us;
                }
                if tuned_hard_age_us == 0 {
                    tuned_hard_age_us = state.v3_durable_confirm_hard_reject_age_us;
                }
                let current_soft_age_us = state
                    .v3_durable_confirm_soft_reject_age_effective_us_per_lane
                    .get(lane_id)
                    .map(|v| {
                        let current = v.load(Ordering::Relaxed);
                        if current > 0 {
                            current
                        } else {
                            tuned_soft_age_us
                        }
                    })
                    .unwrap_or(tuned_soft_age_us);
                let current_hard_age_us = state
                    .v3_durable_confirm_hard_reject_age_effective_us_per_lane
                    .get(lane_id)
                    .map(|v| {
                        let current = v.load(Ordering::Relaxed);
                        if current > 0 {
                            current
                        } else {
                            tuned_hard_age_us
                        }
                    })
                    .unwrap_or(tuned_hard_age_us);
                let slewed_soft_age_us = v3_slew_toward_us(
                    current_soft_age_us,
                    tuned_soft_age_us,
                    CONFIRM_AUTOTUNE_TIGHTEN_STEP_PCT,
                    CONFIRM_AUTOTUNE_RELAX_STEP_PCT,
                    CONFIRM_AUTOTUNE_MIN_STEP_US,
                );
                let mut slewed_hard_age_us = v3_slew_toward_us(
                    current_hard_age_us,
                    tuned_hard_age_us,
                    CONFIRM_AUTOTUNE_TIGHTEN_STEP_PCT,
                    CONFIRM_AUTOTUNE_RELAX_STEP_PCT,
                    CONFIRM_AUTOTUNE_MIN_STEP_US,
                );
                if slewed_soft_age_us > 0
                    && slewed_hard_age_us > 0
                    && slewed_hard_age_us <= slewed_soft_age_us
                {
                    slewed_hard_age_us = slewed_soft_age_us.saturating_add(1);
                    if base_hard_age_us > 0 {
                        slewed_hard_age_us = slewed_hard_age_us.min(base_hard_age_us);
                    }
                }
                if let Some(target) = state
                    .v3_durable_confirm_soft_reject_age_effective_us_per_lane
                    .get(lane_id)
                {
                    target.store(slewed_soft_age_us, Ordering::Relaxed);
                }
                if let Some(target) = state
                    .v3_durable_confirm_hard_reject_age_effective_us_per_lane
                    .get(lane_id)
                {
                    target.store(slewed_hard_age_us, Ordering::Relaxed);
                }
            }
        }
        for lane_id in 0..lane_count {
            let lane_confirm_age_us = confirm_oldest_age_us_per_lane
                .get(lane_id)
                .copied()
                .unwrap_or(confirm_oldest_age_us_max);
            let lane_level = state
                .v3_durable_admission_level_per_lane
                .get(lane_id)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let (mut soft_guard_age_us, mut hard_guard_age_us) =
                state.v3_durable_confirm_reject_ages_for_lane(lane_id);
            if lane_level == 0 {
                soft_guard_age_us = apply_confirm_guard_slack(
                    soft_guard_age_us,
                    state.v3_durable_confirm_guard_soft_slack_pct,
                );
                hard_guard_age_us = apply_confirm_guard_slack(
                    hard_guard_age_us,
                    state.v3_durable_confirm_guard_hard_slack_pct,
                );
            }
            if soft_guard_age_us > 0
                && hard_guard_age_us > 0
                && hard_guard_age_us <= soft_guard_age_us
            {
                hard_guard_age_us = soft_guard_age_us.saturating_add(1);
            }
            let hourly_index = lane_id.saturating_mul(24).saturating_add(hour_slot);
            let pressure_pct = state
                .v3_durable_confirm_hourly_pressure_ewma_per_lane
                .get(hourly_index)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0)
                .min(100);
            let soft_sustain_ticks = if state.v3_durable_confirm_guard_autotune_enabled {
                v3_confirm_guard_effective_ticks(
                    state.v3_durable_confirm_guard_soft_sustain_ticks,
                    pressure_pct,
                    state.v3_durable_confirm_guard_autotune_low_pressure_pct,
                    state.v3_durable_confirm_guard_autotune_high_pressure_pct,
                    1,
                    1,
                )
            } else {
                state.v3_durable_confirm_guard_soft_sustain_ticks
            };
            let hard_sustain_ticks = if state.v3_durable_confirm_guard_autotune_enabled {
                v3_confirm_guard_effective_ticks(
                    state.v3_durable_confirm_guard_hard_sustain_ticks,
                    pressure_pct,
                    state.v3_durable_confirm_guard_autotune_low_pressure_pct,
                    state.v3_durable_confirm_guard_autotune_high_pressure_pct,
                    1,
                    1,
                )
            } else {
                state.v3_durable_confirm_guard_hard_sustain_ticks
            };
            let recover_ticks = if state.v3_durable_confirm_guard_autotune_enabled {
                if pressure_pct >= state.v3_durable_confirm_guard_autotune_high_pressure_pct {
                    state
                        .v3_durable_confirm_guard_recover_ticks
                        .saturating_add(1)
                        .max(1)
                } else if pressure_pct <= state.v3_durable_confirm_guard_autotune_low_pressure_pct {
                    state
                        .v3_durable_confirm_guard_recover_ticks
                        .saturating_sub(1)
                        .max(1)
                } else {
                    state.v3_durable_confirm_guard_recover_ticks
                }
            } else {
                state.v3_durable_confirm_guard_recover_ticks
            };
            if let Some(gauge) = state
                .v3_durable_confirm_guard_soft_sustain_ticks_effective_per_lane
                .get(lane_id)
            {
                gauge.store(soft_sustain_ticks, Ordering::Relaxed);
            }
            if let Some(gauge) = state
                .v3_durable_confirm_guard_hard_sustain_ticks_effective_per_lane
                .get(lane_id)
            {
                gauge.store(hard_sustain_ticks, Ordering::Relaxed);
            }
            if let Some(gauge) = state
                .v3_durable_confirm_guard_recover_ticks_effective_per_lane
                .get(lane_id)
            {
                gauge.store(recover_ticks, Ordering::Relaxed);
            }
            if soft_guard_age_us == 0 {
                confirm_soft_over_streak_per_lane[lane_id] = 0;
                confirm_soft_clear_streak_per_lane[lane_id] = 0;
                if let Some(gauge) = state
                    .v3_durable_confirm_guard_soft_armed_per_lane
                    .get(lane_id)
                {
                    gauge.store(0, Ordering::Relaxed);
                }
            }
            if hard_guard_age_us == 0 {
                confirm_hard_over_streak_per_lane[lane_id] = 0;
                confirm_hard_clear_streak_per_lane[lane_id] = 0;
                if let Some(gauge) = state
                    .v3_durable_confirm_guard_hard_armed_per_lane
                    .get(lane_id)
                {
                    gauge.store(0, Ordering::Relaxed);
                }
            }
            if soft_guard_age_us == 0 && hard_guard_age_us == 0 {
                continue;
            }
            let recover_pct = state
                .v3_durable_confirm_guard_recover_hysteresis_pct
                .min(99);
            let soft_recover_age_us = if soft_guard_age_us > 0 {
                ((soft_guard_age_us as u128)
                    .saturating_mul(recover_pct as u128)
                    .saturating_add(99)
                    / 100) as u64
            } else {
                0
            };
            let hard_recover_age_us = if hard_guard_age_us > 0 {
                ((hard_guard_age_us as u128)
                    .saturating_mul(recover_pct as u128)
                    .saturating_add(99)
                    / 100) as u64
            } else {
                0
            };
            let over_soft = soft_guard_age_us > 0 && lane_confirm_age_us >= soft_guard_age_us;
            let over_hard = hard_guard_age_us > 0 && lane_confirm_age_us >= hard_guard_age_us;
            let clear_soft = soft_guard_age_us > 0 && lane_confirm_age_us <= soft_recover_age_us;
            let clear_hard = hard_guard_age_us > 0 && lane_confirm_age_us <= hard_recover_age_us;
            if over_soft {
                confirm_soft_over_streak_per_lane[lane_id] =
                    confirm_soft_over_streak_per_lane[lane_id].saturating_add(1);
                confirm_soft_clear_streak_per_lane[lane_id] = 0;
            } else if clear_soft {
                confirm_soft_clear_streak_per_lane[lane_id] =
                    confirm_soft_clear_streak_per_lane[lane_id].saturating_add(1);
                confirm_soft_over_streak_per_lane[lane_id] = 0;
            } else {
                confirm_soft_over_streak_per_lane[lane_id] = 0;
                confirm_soft_clear_streak_per_lane[lane_id] = 0;
            }
            if over_hard {
                confirm_hard_over_streak_per_lane[lane_id] =
                    confirm_hard_over_streak_per_lane[lane_id].saturating_add(1);
                confirm_hard_clear_streak_per_lane[lane_id] = 0;
            } else if clear_hard {
                confirm_hard_clear_streak_per_lane[lane_id] =
                    confirm_hard_clear_streak_per_lane[lane_id].saturating_add(1);
                confirm_hard_over_streak_per_lane[lane_id] = 0;
            } else {
                confirm_hard_over_streak_per_lane[lane_id] = 0;
                confirm_hard_clear_streak_per_lane[lane_id] = 0;
            }
            let prev_soft_armed = state
                .v3_durable_confirm_guard_soft_armed_per_lane
                .get(lane_id)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let prev_hard_armed = state
                .v3_durable_confirm_guard_hard_armed_per_lane
                .get(lane_id)
                .map(|v| v.load(Ordering::Relaxed))
                .unwrap_or(0);
            let mut next_soft_armed = prev_soft_armed;
            let mut next_hard_armed = prev_hard_armed;
            if over_soft && confirm_soft_over_streak_per_lane[lane_id] >= soft_sustain_ticks {
                next_soft_armed = 1;
            }
            if over_hard && confirm_hard_over_streak_per_lane[lane_id] >= hard_sustain_ticks {
                next_hard_armed = 1;
            }
            if clear_soft && confirm_soft_clear_streak_per_lane[lane_id] >= recover_ticks {
                next_soft_armed = 0;
            }
            if clear_hard && confirm_hard_clear_streak_per_lane[lane_id] >= recover_ticks {
                next_hard_armed = 0;
            }
            if next_hard_armed > 0 {
                next_soft_armed = 1;
            }
            if let Some(gauge) = state
                .v3_durable_confirm_guard_soft_armed_per_lane
                .get(lane_id)
            {
                gauge.store(next_soft_armed, Ordering::Relaxed);
            }
            if let Some(gauge) = state
                .v3_durable_confirm_guard_hard_armed_per_lane
                .get(lane_id)
            {
                gauge.store(next_hard_armed, Ordering::Relaxed);
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
                let lane_fsync_p99_us = fsync_p99_per_lane
                    .get(lane_id)
                    .copied()
                    .unwrap_or(fsync_p99_global);
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
                if fsync_soft {
                    fsync_soft_streak_per_lane[lane_id] =
                        fsync_soft_streak_per_lane[lane_id].saturating_add(1);
                } else {
                    fsync_soft_streak_per_lane[lane_id] = 0;
                }
                if fsync_hard {
                    fsync_hard_streak_per_lane[lane_id] =
                        fsync_hard_streak_per_lane[lane_id].saturating_add(1);
                } else {
                    fsync_hard_streak_per_lane[lane_id] = 0;
                }
                let fsync_only_soft = fsync_only_soft_sustain_ticks > 0
                    && fsync_soft
                    && fsync_soft_streak_per_lane[lane_id] >= fsync_only_soft_sustain_ticks;
                let fsync_only_hard = fsync_only_hard_sustain_ticks > 0
                    && fsync_hard
                    && fsync_hard_streak_per_lane[lane_id] >= fsync_only_hard_sustain_ticks;
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
                    || ((queue_pressure_soft || backlog_pressure_soft) && fsync_hard)
                    || fsync_only_hard;
                let soft_signal = queue_pressure_soft
                    || backlog_pressure_soft
                    || ((lane_queue_pct_now >= fsync_presignal_queue_pct
                        || (backlog_signal_enabled && lane_growth >= fsync_presignal_backlog))
                        && fsync_soft)
                    || fsync_only_soft;

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
        account_id: &str,
        session_seq: u64,
        received_at_ns: u64,
        shard_id: usize,
    ) -> V3OrderTask {
        V3OrderTask {
            session_id: Arc::<str>::from(session_id),
            account_id: Arc::<str>::from(account_id),
            position_symbol_key: FastPathEngine::symbol_to_bytes("AAPL"),
            position_delta_qty: 1,
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
                session_id: Arc::<str>::from(format!("sess-{}", shard_id)),
                account_id: Arc::<str>::from(format!("acc-{}", shard_id)),
                position_symbol_key: FastPathEngine::symbol_to_bytes("AAPL"),
                position_delta_qty: 1,
                session_seq: shard_id as u64,
                attempt_seq: shard_id as u64,
                received_at_ns: 1_000_000,
                shard_id,
            };
            ingress
                .try_enqueue(shard_id, task)
                .expect("enqueue should succeed for test lane capacity");
        }

        assert_eq!(ingress.lane_depths(), vec![2, 2, 2, 2]);
    }

    #[test]
    fn v3_ingress_seed_methods_keep_floor_and_shard_binding() {
        let mut shards = Vec::new();
        let mut _rxs = Vec::new();
        for _ in 0..2 {
            let (tx, rx) = tokio::sync::mpsc::channel(8);
            shards.push(V3ShardIngress::new(tx, 8));
            _rxs.push(rx);
        }
        let ingress = V3Ingress::new(shards, false, 95, 10, 5, 32, 64, 128);

        assert!(ingress.seed_next_seq_floor("sess-a", 10));
        assert!(!ingress.seed_next_seq_floor("sess-a", 5));
        assert_eq!(ingress.next_seq("sess-a"), 10);

        assert!(ingress.seed_session_shard("sess-a", 1));
        assert!(!ingress.seed_session_shard("sess-a", 0));
        assert_eq!(ingress.shard_for_session("sess-a"), 1);

        assert!(ingress.seed_session_shard("sess-b", 99));
        assert_eq!(ingress.shard_for_session("sess-b"), 1);
    }

    #[test]
    fn v3_confirm_store_lane_hash_is_stable_and_skew_is_observable() {
        let store = V3ConfirmStore::new(4, 500, 60_000);
        let lane_a = store.lane_for_session("sess-a");
        let lane_b = store.lane_for_session("sess-a");
        assert_eq!(lane_a, lane_b);
        assert_eq!(store.lane_count_metric(), 4);

        for seq in 0..8 {
            let task = test_task("sess-a", "acc-a", seq, 1_000_000, 0);
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
            let task = test_task(
                &format!("sess-{}", seq),
                "acc-wheel",
                seq,
                far_future_ns,
                0,
            );
            store.record_volatile(&task, far_future_ns);
        }
        let pre_scan = store.collect_timed_out(1_000_000, 256);
        assert!(pre_scan.candidates.is_empty());
        assert_eq!(pre_scan.scan_cost, 0);

        let due_received_ns = 2_000_000u64;
        let due = test_task("sess-due", "acc-due", 1, due_received_ns, 2);
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
        let task = test_task("sess-gc", "acc-gc", 7, received_ns, 0);
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
    fn collect_v3_rebuild_records_parses_runtime_fields_and_max_seq() {
        let symbol_key = FastPathEngine::symbol_to_bytes("AAPL");
        let symbol_key_u64 = u64::from_le_bytes(symbol_key);
        let events = vec![
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 1,
                account_id: "acc-a".to_string(),
                order_id: Some("v3/sess-a/5".to_string()),
                data: json!({
                    "positionSymbolKey": symbol_key_u64.to_string(),
                    "positionDeltaQty": "7",
                    "shardId": "3",
                }),
            })
            .expect("serialize accepted event a5"),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableRejected".to_string(),
                at: 2,
                account_id: "acc-b".to_string(),
                order_id: Some("v3/sess-b/2".to_string()),
                data: json!({
                    "reason": "RISK_REJECTED",
                    "shardId": 1,
                }),
            })
            .expect("serialize rejected event b2"),
            serde_json::to_string(&AuditEvent {
                event_type: "V3DurableAccepted".to_string(),
                at: 3,
                account_id: "acc-a".to_string(),
                order_id: Some("v3/sess-a/6".to_string()),
                data: json!({
                    "positionSymbolKey": symbol_key_u64,
                    "positionDeltaQty": -4,
                    "shardId": 4,
                }),
            })
            .expect("serialize accepted event a6"),
        ]
        .join("\n");
        let reader = BufReader::new(Cursor::new(events.into_bytes()));
        let (records, max_seq_per_session) = collect_v3_rebuild_records_from_reader(reader, 1024);

        assert_eq!(records.len(), 3);
        assert_eq!(max_seq_per_session.get("sess-a"), Some(&6));
        assert_eq!(max_seq_per_session.get("sess-b"), Some(&2));

        let accepted = records
            .get(&("sess-a".to_string(), 6))
            .expect("sess-a/6 must exist");
        assert_eq!(accepted.status, V3ConfirmStatus::DurableAccepted);
        assert_eq!(accepted.account_id, "acc-a");
        assert_eq!(accepted.position_symbol_key, Some(symbol_key));
        assert_eq!(accepted.position_delta_qty, Some(-4));
        assert_eq!(accepted.shard_id, Some(4));

        let rejected = records
            .get(&("sess-b".to_string(), 2))
            .expect("sess-b/2 must exist");
        assert_eq!(rejected.status, V3ConfirmStatus::DurableRejected);
        assert_eq!(rejected.reason.as_deref(), Some("RISK_REJECTED"));
        assert_eq!(rejected.position_symbol_key, None);
        assert_eq!(rejected.position_delta_qty, None);
        assert_eq!(rejected.shard_id, Some(1));
    }

    #[test]
    fn v3_durable_confirm_age_target_tightens_as_pressure_rises() {
        let min_us = 70_000;
        let max_us = 140_000;
        assert_eq!(v3_durable_confirm_age_target_us(min_us, max_us, 0), max_us);
        assert_eq!(
            v3_durable_confirm_age_target_us(min_us, max_us, 100),
            min_us
        );
        let mid = v3_durable_confirm_age_target_us(min_us, max_us, 50);
        assert!(mid < max_us);
        assert!(mid > min_us);
    }

    #[test]
    fn v3_pressure_ewma_u64_initializes_and_smooths() {
        assert_eq!(v3_pressure_ewma_u64(0, 80, 20), 80);
        let next = v3_pressure_ewma_u64(80, 20, 20);
        assert!(next < 80);
        assert!(next > 20);
    }

    #[test]
    fn v3_slew_toward_us_tightens_faster_than_relaxes() {
        let current = 200_000;
        let tightened = v3_slew_toward_us(current, 120_000, 20, 2, 1_000);
        let relaxed = v3_slew_toward_us(current, 260_000, 20, 2, 1_000);
        assert_eq!(tightened, 160_000);
        assert_eq!(relaxed, 204_000);
        assert!(tightened < current);
        assert!(relaxed > current);
        assert!(current - tightened > relaxed - current);
    }

    #[test]
    fn v3_confirm_autotune_queue_pressure_is_gated_in_safe_zone() {
        assert_eq!(v3_confirm_autotune_queue_pressure_pct(92, 0, 0), 0);
        assert_eq!(v3_confirm_autotune_queue_pressure_pct(92, 1, 0), 92);
        assert_eq!(v3_confirm_autotune_queue_pressure_pct(92, 0, 1), 92);
    }

    #[test]
    fn v3_confirm_autotune_admission_floor_maps_levels() {
        assert_eq!(v3_confirm_autotune_admission_floor_pct(0), 0);
        assert_eq!(v3_confirm_autotune_admission_floor_pct(1), 70);
        assert_eq!(v3_confirm_autotune_admission_floor_pct(2), 90);
        assert_eq!(v3_confirm_autotune_admission_floor_pct(9), 90);
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
