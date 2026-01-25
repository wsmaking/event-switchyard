//! 動的 inflight 制御（Soft Reject 用）
//!
//! durable の commit_rate と wal_age 目標から inflight 上限を自動調整する。
//! Ingress は最新の上限を読み、閾値超過時に 429 を返す。

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct InflightControllerHandle {
    limit: Arc<AtomicU64>,
    rejecting: Arc<AtomicBool>,
    committed_total: Arc<AtomicU64>,
    rate_ewma_milli: Arc<AtomicU64>,
    hysteresis_off_ratio: f64,
}

impl InflightControllerHandle {
    fn new(config: &InflightControllerConfig) -> Self {
        let initial_limit = config.initial_limit.max(1);
        Self {
            limit: Arc::new(AtomicU64::new(initial_limit)),
            rejecting: Arc::new(AtomicBool::new(false)),
            committed_total: Arc::new(AtomicU64::new(0)),
            rate_ewma_milli: Arc::new(AtomicU64::new(0)),
            hysteresis_off_ratio: config.hysteresis_off_ratio,
        }
    }

    pub fn record_commit(&self, n: u64) {
        if n > 0 {
            self.committed_total.fetch_add(n, Ordering::Relaxed);
        }
    }

    pub fn should_soft_reject(&self, inflight: u64) -> bool {
        let limit = self.limit.load(Ordering::Relaxed).max(1);
        let off_threshold = (limit as f64 * self.hysteresis_off_ratio) as u64;
        let rejecting = self.rejecting.load(Ordering::Relaxed);
        let next = if rejecting {
            inflight >= off_threshold
        } else {
            inflight >= limit
        };
        if next != rejecting {
            self.rejecting.store(next, Ordering::Relaxed);
        }
        next
    }

    pub fn limit(&self) -> u64 {
        self.limit.load(Ordering::Relaxed)
    }

    pub fn rate_ewma_milli(&self) -> u64 {
        self.rate_ewma_milli.load(Ordering::Relaxed)
    }
}

pub struct InflightControllerConfig {
    pub enabled: bool,
    pub target_wal_age_sec: f64,
    pub safety_alpha: f64,
    pub beta: f64,
    pub inflight_min: u64,
    pub inflight_cap: u64,
    pub tick_ms: u64,
    pub max_step_ratio: f64,
    pub hysteresis_off_ratio: f64,
    pub initial_limit: u64,
}

impl InflightControllerConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: parse_bool_env("BACKPRESSURE_INFLIGHT_DYNAMIC").unwrap_or(false),
            target_wal_age_sec: parse_f64_env("BACKPRESSURE_INFLIGHT_TARGET_WAL_AGE_SEC")
                .unwrap_or(1.0),
            safety_alpha: parse_f64_env("BACKPRESSURE_INFLIGHT_ALPHA").unwrap_or(0.8),
            beta: parse_f64_env("BACKPRESSURE_INFLIGHT_BETA").unwrap_or(0.2),
            inflight_min: parse_u64_env("BACKPRESSURE_INFLIGHT_MIN").unwrap_or(256),
            inflight_cap: parse_u64_env("BACKPRESSURE_INFLIGHT_CAP").unwrap_or(10_000),
            tick_ms: parse_u64_env("BACKPRESSURE_INFLIGHT_TICK_MS").unwrap_or(250),
            max_step_ratio: parse_f64_env("BACKPRESSURE_INFLIGHT_SLEW_RATIO").unwrap_or(0.2),
            hysteresis_off_ratio: parse_f64_env("BACKPRESSURE_INFLIGHT_HYSTERESIS_OFF_RATIO")
                .unwrap_or(0.9),
            initial_limit: parse_u64_env("BACKPRESSURE_INFLIGHT_INITIAL").unwrap_or(2_000),
        }
    }
}

pub struct InflightController {
    config: InflightControllerConfig,
    handle: InflightControllerHandle,
    last_sample: Instant,
    last_committed_total: u64,
    rate_ewma: f64,
}

impl InflightController {
    pub fn spawn_from_env() -> Option<InflightControllerHandle> {
        let config = InflightControllerConfig::from_env();
        if !config.enabled {
            return None;
        }
        let handle = InflightControllerHandle::new(&config);
        let controller = Self::new(config, handle.clone());
        tokio::spawn(async move {
            controller.run().await;
        });
        Some(handle)
    }

    fn new(config: InflightControllerConfig, handle: InflightControllerHandle) -> Self {
        let target = if config.target_wal_age_sec > 0.0 {
            config.target_wal_age_sec
        } else {
            1.0
        };
        let rate_ewma = (config.initial_limit as f64) / target;
        handle
            .rate_ewma_milli
            .store((rate_ewma * 1000.0) as u64, Ordering::Relaxed);
        Self {
            config,
            handle,
            last_sample: Instant::now(),
            last_committed_total: 0,
            rate_ewma,
        }
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(self.config.tick_ms));
        loop {
            interval.tick().await;
            self.tick();
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_sample);
        if dt < Duration::from_millis(50) {
            return;
        }
        let dt_sec = dt.as_secs_f64().max(0.000_001);
        let committed_total = self.handle.committed_total.load(Ordering::Relaxed);
        let committed_delta = committed_total.saturating_sub(self.last_committed_total);
        let instant_rate = (committed_delta as f64) / dt_sec;

        self.rate_ewma = (1.0 - self.config.beta) * self.rate_ewma
            + self.config.beta * instant_rate;
        self.handle
            .rate_ewma_milli
            .store((self.rate_ewma * 1000.0) as u64, Ordering::Relaxed);

        let target = self.config.target_wal_age_sec.max(0.001);
        let raw = self.config.safety_alpha * self.rate_ewma * target;
        let mut desired = raw.round() as u64;
        desired = desired.clamp(self.config.inflight_min, self.config.inflight_cap);

        let current = self.handle.limit.load(Ordering::Relaxed).max(1);
        let max_step = ((current as f64) * self.config.max_step_ratio).max(1.0) as u64;
        let next = if desired > current + max_step {
            current + max_step
        } else if desired + max_step < current {
            current.saturating_sub(max_step)
        } else {
            desired
        };

        if next != current {
            self.handle.limit.store(next, Ordering::Relaxed);
        }

        self.last_committed_total = committed_total;
        self.last_sample = now;
    }
}

fn parse_u64_env(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|v| v.parse::<u64>().ok())
}

fn parse_f64_env(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|v| v.parse::<f64>().ok())
}

fn parse_bool_env(key: &str) -> Option<bool> {
    std::env::var(key).ok().and_then(|v| match v.to_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    })
}
