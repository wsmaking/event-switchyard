use super::{
    AppState, V3ConfirmStatus, V3ConfirmStore, parse_v3_i64_data_field, parse_v3_order_id,
    parse_v3_string_data_field, parse_v3_u64_data_field,
};
use crate::audit::AuditEvent;
use crate::strategy::runtime::{AlgoParentExecution, STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::time::Instant;
use tracing::info;

use super::strategy::{append_algo_runtime_snapshot, spawn_algo_runtime};

#[derive(Debug, Clone)]
pub(super) struct V3RebuildRecord {
    pub(super) status: V3ConfirmStatus,
    pub(super) reason: Option<String>,
    pub(super) at_ns: u64,
    pub(super) account_id: String,
    pub(super) intent_id: Option<String>,
    pub(super) position_symbol_key: Option<[u8; 8]>,
    pub(super) position_delta_qty: Option<i64>,
    pub(super) shard_id: Option<usize>,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct V3RebuildStats {
    pub(super) confirm_restored: u64,
    pub(super) position_applied: u64,
    pub(super) session_seq_seeded: u64,
    pub(super) session_shard_seeded: u64,
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct StrategyRuntimeRebuildStats {
    pub(super) parent_restored: u64,
    pub(super) durable_child_replayed: u64,
    pub(super) resumed_children: u64,
}

pub(super) fn collect_v3_rebuild_records_from_reader<R: BufRead>(
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
                        intent_id: parse_v3_string_data_field(&event.data, "intentId"),
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
                        intent_id: parse_v3_string_data_field(&event.data, "intentId"),
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
                state.v3_confirm_store.mark_durable_accepted(
                    &session_id,
                    session_seq,
                    record.at_ns,
                );
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

pub(super) fn rebuild_v3_confirm_store_from_reader<R: BufRead>(
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

fn collect_strategy_runtime_snapshots_from_reader<R: BufRead>(
    reader: R,
    max_lines: usize,
) -> HashMap<String, AlgoParentExecution> {
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

    let mut snapshots = HashMap::new();
    for event in ring {
        if event.event_type != STRATEGY_ALGO_RUNTIME_SNAPSHOT_EVENT_TYPE {
            continue;
        }
        let runtime: AlgoParentExecution = match serde_json::from_value(event.data) {
            Ok(runtime) => runtime,
            Err(_) => continue,
        };
        snapshots.insert(runtime.parent_intent_id.clone(), runtime);
    }
    snapshots
}

fn rebuild_strategy_runtime_store_from_wal_path(
    state: &AppState,
    path: &Path,
    max_lines: usize,
) -> u64 {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    let reader = BufReader::new(file);
    let snapshots = collect_strategy_runtime_snapshots_from_reader(reader, max_lines);
    let mut restored = 0u64;
    for runtime in snapshots.into_values() {
        if state.strategy_runtime_store.insert(runtime).is_ok() {
            restored = restored.saturating_add(1);
        }
    }
    restored
}

fn replay_strategy_runtime_durable_outcomes_from_wal_path(
    state: &AppState,
    path: &Path,
    max_lines: usize,
) -> u64 {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    let reader = BufReader::new(file);
    let (records, _) = collect_v3_rebuild_records_from_reader(reader, max_lines);
    let mut replayed = 0u64;

    for (_key, record) in records {
        let Some(intent_id) = record.intent_id.as_deref() else {
            continue;
        };
        if state
            .strategy_runtime_store
            .get_by_child(intent_id)
            .is_none()
        {
            continue;
        }
        match record.status {
            V3ConfirmStatus::DurableAccepted => {
                let _ = state
                    .strategy_runtime_store
                    .record_child_durable_accepted(intent_id, record.at_ns);
                replayed = replayed.saturating_add(1);
            }
            V3ConfirmStatus::DurableRejected => {
                let reason = record.reason.as_deref().unwrap_or("WAL_DURABILITY_FAILED");
                let _ = state.strategy_runtime_store.record_child_durable_rejected(
                    intent_id,
                    record.at_ns,
                    reason,
                );
                replayed = replayed.saturating_add(1);
            }
            _ => {}
        }
    }

    replayed
}

pub(super) fn rebuild_strategy_runtime_from_wal(
    state: &AppState,
    max_lines: usize,
) -> StrategyRuntimeRebuildStats {
    let mut stats = StrategyRuntimeRebuildStats::default();
    stats.parent_restored = rebuild_strategy_runtime_store_from_wal_path(
        state,
        state.audit_read_path.as_ref().as_path(),
        max_lines,
    );
    for path in state.v3_confirm_rebuild_paths.iter() {
        stats.durable_child_replayed = stats.durable_child_replayed.saturating_add(
            replay_strategy_runtime_durable_outcomes_from_wal_path(state, path, max_lines),
        );
    }
    for runtime in state.strategy_runtime_store.list() {
        if runtime.is_terminal() {
            continue;
        }
        if matches!(
            runtime.runtime_mode,
            crate::strategy::runtime::AlgoRuntimeMode::PauseOnRestart
        ) {
            let paused_at_ns = gateway_core::now_nanos();
            if state.strategy_runtime_store.pause_parent_on_restart(
                &runtime.parent_intent_id,
                paused_at_ns,
                "STRATEGY_NO_AUTO_RESUME_ON_RESTART",
            ) {
                if let Some(parent) = state.strategy_runtime_store.get(&runtime.parent_intent_id) {
                    append_algo_runtime_snapshot(state, &parent);
                }
            }
            continue;
        }
        let scheduled = runtime.scheduled_child_count();
        if scheduled == 0 {
            continue;
        }
        spawn_algo_runtime(state.clone(), &runtime);
        stats.resumed_children = stats.resumed_children.saturating_add(scheduled);
    }
    stats
}

pub(super) fn record_startup_rebuild_stats(
    state: &AppState,
    v3_stats: V3RebuildStats,
    strategy_stats: StrategyRuntimeRebuildStats,
    elapsed_ms: u64,
    max_lines: usize,
) {
    state.v3_confirm_rebuild_restored_total.store(
        v3_stats.confirm_restored,
        std::sync::atomic::Ordering::Relaxed,
    );
    state
        .v3_confirm_rebuild_elapsed_ms
        .store(elapsed_ms, std::sync::atomic::Ordering::Relaxed);
    state.v3_replay_position_applied_total.store(
        v3_stats.position_applied,
        std::sync::atomic::Ordering::Relaxed,
    );
    state.v3_replay_session_seq_seeded_total.store(
        v3_stats.session_seq_seeded,
        std::sync::atomic::Ordering::Relaxed,
    );
    state.v3_replay_session_shard_seeded_total.store(
        v3_stats.session_shard_seeded,
        std::sync::atomic::Ordering::Relaxed,
    );
    info!(
        restored = v3_stats.confirm_restored,
        position_applied = v3_stats.position_applied,
        session_seq_seeded = v3_stats.session_seq_seeded,
        session_shard_seeded = v3_stats.session_shard_seeded,
        elapsed_ms = elapsed_ms,
        max_lines = max_lines,
        "v3 runtime state rebuilt from WAL"
    );
    info!(
        parent_restored = strategy_stats.parent_restored,
        durable_child_replayed = strategy_stats.durable_child_replayed,
        resumed_children = strategy_stats.resumed_children,
        elapsed_ms = elapsed_ms,
        max_lines = max_lines,
        "strategy algo runtime rebuilt from WAL"
    );
}

pub(super) fn run_startup_rebuild_sync(
    state: &AppState,
    max_lines: usize,
) -> (V3RebuildStats, StrategyRuntimeRebuildStats, u64) {
    let rebuild_t0 = Instant::now();
    let v3_stats = rebuild_v3_confirm_store_from_wal(state, max_lines);
    let strategy_stats = rebuild_strategy_runtime_from_wal(state, max_lines);
    let elapsed_ms = rebuild_t0.elapsed().as_millis() as u64;
    (v3_stats, strategy_stats, elapsed_ms)
}

pub(super) async fn run_startup_rebuild_task(state: AppState, max_lines: usize) {
    info!(max_lines = max_lines, "startup WAL rebuild started");
    let (v3_stats, strategy_stats, elapsed_ms) =
        tokio::task::block_in_place(|| run_startup_rebuild_sync(&state, max_lines));
    record_startup_rebuild_stats(&state, v3_stats, strategy_stats, elapsed_ms, max_lines);
    state.mark_v3_startup_rebuild_completed(gateway_core::now_nanos());
    info!(elapsed_ms = elapsed_ms, "startup WAL rebuild completed");
}
