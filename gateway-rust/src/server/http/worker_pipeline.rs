use super::*;

pub(super) async fn run_v3_single_writer_impl(
    shard_id: usize,
    mut rx: Receiver<V3OrderTask>,
    state: AppState,
) {
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
        maybe_append_strategy_execution_fact(
            &state,
            task.execution_run_id.as_deref(),
            task.decision_key.as_deref(),
            task.decision_attempt_seq,
            task.intent_id.as_deref(),
            task.model_id.as_deref(),
            task.account_id.as_ref(),
            task.session_id.as_ref(),
            Some(task.session_seq),
            crate::server::http::orders::render_v3_symbol_key(task.position_symbol_key).as_str(),
            Some(task.position_delta_qty),
            task.received_at_ns,
            StrategyExecutionFactStatus::Unconfirmed,
            None,
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

pub(super) async fn run_v3_durable_worker_impl(
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
        let mut out = Vec::with_capacity(
            192 + task.session_id.len() + task.account_id.len().saturating_mul(2),
        );
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
        if let Some(intent_id) = task.intent_id.as_deref() {
            out.extend_from_slice(b",\"intentId\":");
            push_json_string(&mut out, intent_id);
        }
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
            account_id,
            execution_run_id,
            decision_key,
            decision_attempt_seq,
            intent_id,
            model_id,
            effective_risk_budget_ref,
            actual_policy,
            position_symbol_key,
            position_delta_qty,
            session_seq,
            received_at_ns,
            ..
        } = task;
        if state.quant_feedback_exporter.is_enabled() {
            let symbol = render_v3_symbol_key(position_symbol_key);
            let event = FeedbackEvent::accepted(
                session_id.as_ref(),
                session_seq,
                account_id.as_ref(),
                symbol,
                received_at_ns,
            )
            .push_path_tag("v3")
            .push_path_tag("durable")
            .push_path_tag("feedback");
            let event = if let Some(execution_run_id) = execution_run_id.as_deref() {
                event.with_execution_run_id(execution_run_id)
            } else {
                event
            };
            let event = if let Some(decision_key) = decision_key.as_deref() {
                event.with_decision_key(decision_key)
            } else {
                event
            };
            let event = if let Some(decision_attempt_seq) = decision_attempt_seq {
                event.with_decision_attempt_seq(decision_attempt_seq)
            } else {
                event
            };
            let event = if let Some(intent_id) = intent_id.as_deref() {
                event.with_intent_id(intent_id)
            } else {
                event
            };
            let event = if let Some(model_id) = model_id.as_deref() {
                event.with_model_id(model_id)
            } else {
                event
            };
            let event =
                if let Some(effective_risk_budget_ref) = effective_risk_budget_ref.as_deref() {
                    event.with_effective_risk_budget_ref(effective_risk_budget_ref)
                } else {
                    event
                };
            let event = if let Some(actual_policy) = actual_policy.as_deref() {
                event.with_actual_policy(actual_policy.clone())
            } else {
                event
            };
            let event = if outcome.durable_done_ns > 0 {
                event.durable_accepted(outcome.durable_done_ns)
            } else {
                event.durable_rejected(now_ns, outcome.reject_reason)
            };
            publish_quant_feedback(&state, event);
        }
        maybe_append_strategy_execution_fact(
            &state,
            execution_run_id.as_deref(),
            decision_key.as_deref(),
            decision_attempt_seq,
            intent_id.as_deref(),
            model_id.as_deref(),
            account_id.as_ref(),
            session_id.as_ref(),
            Some(session_seq),
            render_v3_symbol_key(position_symbol_key).as_str(),
            Some(position_delta_qty),
            if outcome.durable_done_ns > 0 {
                outcome.durable_done_ns
            } else {
                now_ns
            },
            if outcome.durable_done_ns > 0 {
                StrategyExecutionFactStatus::DurableAccepted
            } else {
                StrategyExecutionFactStatus::DurableRejected
            },
            (outcome.durable_done_ns == 0).then_some(outcome.reject_reason),
        );
        if let Some(intent_id) = intent_id.as_deref() {
            let parent_event = if outcome.durable_done_ns > 0 {
                state
                    .strategy_runtime_store
                    .record_child_durable_accepted(intent_id, outcome.durable_done_ns)
            } else {
                state.strategy_runtime_store.record_child_durable_rejected(
                    intent_id,
                    now_ns,
                    outcome.reject_reason,
                )
            };
            if let Some(parent) = state.strategy_runtime_store.get_by_child(intent_id) {
                append_algo_runtime_snapshot(&state, &parent);
            }
            if let Some(event) = parent_event {
                publish_quant_feedback(
                    &state,
                    event
                        .push_path_tag("strategy")
                        .push_path_tag("algo_parent")
                        .push_path_tag("feedback"),
                );
            }
        }
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
        if soft_pressure_age_us > 0 && confirm_oldest_age_us_lane >= soft_pressure_age_us {
            let soft_cap = (((max_inflight_receipts as u128)
                .saturating_mul(age_soft_inflight_cap_pct as u128)
                / 100) as usize)
                .max(fallback_batch_max)
                .min(max_inflight_receipts);
            effective_max_inflight = effective_max_inflight.min(soft_cap);
        }
        if hard_pressure_age_us > 0 && confirm_oldest_age_us_lane >= hard_pressure_age_us {
            let hard_cap = (((max_inflight_receipts as u128)
                .saturating_mul(age_hard_inflight_cap_pct as u128)
                / 100) as usize)
                .max(fallback_batch_max)
                .min(max_inflight_receipts);
            effective_max_inflight = effective_max_inflight.min(hard_cap);
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
                    if let Some(replica_timings) = replica_best_effort_timings
                        && replica_timings.enqueue_done_ns == 0
                        && replica_timings.durable_done_ns == 0
                    {
                        state
                            .v3_durable_replica_write_error_total
                            .fetch_add(1, Ordering::Relaxed);
                        state
                            .v3_durable_write_error_total
                            .fetch_add(1, Ordering::Relaxed);
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
