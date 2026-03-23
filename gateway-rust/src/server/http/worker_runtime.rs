use super::*;

pub(super) async fn run_v3_loss_monitor(state: AppState) {
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

pub(super) async fn run_durable_notifier(
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
