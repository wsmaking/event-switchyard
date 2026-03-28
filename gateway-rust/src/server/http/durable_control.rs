use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
pub(super) struct V3DurableWorkerBatchAdaptiveConfig {
    pub(super) enabled: bool,
    pub(super) batch_min: usize,
    pub(super) batch_max: usize,
    pub(super) wait_min: Duration,
    pub(super) wait_max: Duration,
    pub(super) low_util_pct: f64,
    pub(super) high_util_pct: f64,
}

impl V3DurableWorkerBatchAdaptiveConfig {
    pub(super) fn target_for_pressure(&self, pressure_pct: f64) -> (usize, Duration) {
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
pub(super) enum V3DurableControlPreset {
    Legacy,
    HftStable,
}

impl V3DurableControlPreset {
    pub(super) fn from_env() -> Self {
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

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::HftStable => "hft_stable",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct V3DurableWorkerPressureConfig {
    pub(super) control_preset: V3DurableControlPreset,
    pub(super) durable_slo_stage: u64,
    pub(super) dynamic_inflight_enabled: bool,
    pub(super) dynamic_inflight_min_cap_pct: u64,
    pub(super) dynamic_inflight_max_cap_pct: u64,
    pub(super) dynamic_inflight_strict_max_cap_pct: u64,
    pub(super) early_soft_age_us: u64,
    pub(super) early_hard_age_us: u64,
    pub(super) age_soft_inflight_cap_pct: u64,
    pub(super) age_hard_inflight_cap_pct: u64,
    pub(super) fsync_soft_inflight_cap_pct: u64,
    pub(super) fsync_hard_inflight_cap_pct: u64,
    pub(super) fsync_soft_trigger_us: u64,
    pub(super) fsync_hard_trigger_us: u64,
    pub(super) pressure_ewma_alpha_pct: u64,
    pub(super) dynamic_cap_slew_step_pct: u64,
}

impl V3DurableWorkerPressureConfig {
    pub(super) fn from_env(inflight_hard_cap_pct: u64) -> Self {
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
        let dynamic_inflight_enabled = super::parse_bool_env("V3_DURABLE_WORKER_DYNAMIC_INFLIGHT")
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

pub(super) fn v3_pressure_ratio(current: i64, soft: i64, hard: i64) -> f64 {
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

pub(super) fn v3_fsync_pressure_ratio(current_us: u64, soft_us: u64, hard_us: u64) -> f64 {
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

pub(super) fn v3_confirm_age_pressure_ratio(current_us: u64, soft_us: u64, hard_us: u64) -> f64 {
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
pub(super) fn v3_pressure_ewma_u64(prev: u64, sample: u64, alpha_pct: u64) -> u64 {
    let alpha = alpha_pct.clamp(1, 100);
    if prev == 0 {
        return sample.min(100);
    }
    let keep = 100u128.saturating_sub(alpha as u128);
    (((prev as u128).saturating_mul(keep) + (sample as u128).saturating_mul(alpha as u128) + 50)
        / 100) as u64
}

#[inline]
pub(super) fn v3_ewma_u64(prev: u64, sample: u64, alpha_pct: u64) -> u64 {
    let alpha = alpha_pct.clamp(1, 100);
    if prev == 0 {
        return sample;
    }
    let keep = 100u128.saturating_sub(alpha as u128);
    (((prev as u128).saturating_mul(keep) + (sample as u128).saturating_mul(alpha as u128) + 50)
        / 100) as u64
}

#[inline]
pub(super) fn v3_durable_confirm_age_target_us(min_us: u64, max_us: u64, pressure_pct: u64) -> u64 {
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
pub(super) fn v3_confirm_autotune_admission_floor_pct(admission_level: u64) -> u64 {
    match admission_level {
        0 => 0,
        1 => 70,
        _ => 90,
    }
}

#[inline]
pub(super) fn v3_confirm_autotune_queue_pressure_pct(
    lane_pressure_pct: u64,
    age_pressure_pct: u64,
    admission_level: u64,
) -> u64 {
    if age_pressure_pct > 0 || admission_level > 0 {
        lane_pressure_pct.min(100)
    } else {
        0
    }
}

#[inline]
pub(super) fn apply_confirm_guard_slack(age_us: u64, slack_pct: u64) -> u64 {
    if age_us == 0 || slack_pct == 0 {
        age_us
    } else {
        age_us.saturating_add(((age_us as u128).saturating_mul(slack_pct as u128) / 100) as u64)
    }
}

#[inline]
pub(super) fn v3_confirm_guard_effective_ticks(
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
pub(super) fn v3_slew_toward_us(
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
pub(super) fn current_utc_hour_slot() -> usize {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| ((d.as_secs() / 3_600) % 24) as usize)
        .unwrap_or(0)
}

pub(super) fn v3_durable_slo_age_targets(stage: u64, preset: V3DurableControlPreset) -> (u64, u64) {
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

pub(super) fn choose_tighter_age_target(preferred: u64, fallback: u64) -> u64 {
    match (preferred, fallback) {
        (0, 0) => 0,
        (a, 0) => a,
        (0, b) => b,
        (a, b) => a.min(b),
    }
}
