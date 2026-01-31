//! 動的 inflight 制御（single-writer）
//!
//! Controller が inflight/limit/rate を一元管理し、HTTP 側は watch で参照する。

use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot, watch};

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
pub enum ReserveDecision {
    Allow { inflight: u64, limit: u64 },
    RejectInflight { inflight: u64, limit: u64 },
    RejectRateDecline { inflight: u64, limit: u64 },
}

#[derive(Clone)]
pub struct InflightControllerHandle {
    enabled: bool,
    cmd_tx: mpsc::UnboundedSender<InflightCommand>,
    inflight_rx: watch::Receiver<u64>,
    limit_rx: watch::Receiver<u64>,
    rate_ewma_milli_rx: watch::Receiver<u64>,
    soft_reject_rate_milli_rx: watch::Receiver<u64>,
    #[allow(dead_code)]
    rate_declining_rx: watch::Receiver<bool>,
}

impl InflightControllerHandle {
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub fn inflight(&self) -> u64 {
        *self.inflight_rx.borrow()
    }

    pub fn current_limit(&self) -> u64 {
        *self.limit_rx.borrow()
    }

    pub fn rate_ewma_milli(&self) -> u64 {
        *self.rate_ewma_milli_rx.borrow()
    }

    pub fn soft_reject_rate_ewma_milli(&self) -> u64 {
        *self.soft_reject_rate_milli_rx.borrow()
    }

    #[allow(dead_code)]
    pub fn rate_declining(&self) -> bool {
        *self.rate_declining_rx.borrow()
    }

    pub async fn reserve(&self) -> ReserveDecision {
        let (tx, rx) = oneshot::channel();
        let _ = self.cmd_tx.send(InflightCommand::Reserve { resp: tx });
        rx.await.unwrap_or(ReserveDecision::RejectInflight {
            inflight: self.inflight(),
            limit: self.current_limit(),
        })
    }

    pub fn release(&self, n: u64) {
        let _ = self.cmd_tx.send(InflightCommand::Release { n });
    }

    pub fn on_commit(&self, n: u64) {
        let _ = self.cmd_tx.send(InflightCommand::Commit { n });
    }
}

#[derive(Debug)]
enum InflightCommand {
    Reserve { resp: oneshot::Sender<ReserveDecision> },
    Release { n: u64 },
    Commit { n: u64 },
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
    pub decline_ratio: f64,
    pub decline_streak: u64,
    pub reject_rate_beta: f64,
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
            decline_ratio: parse_f64_env("BACKPRESSURE_INFLIGHT_DECLINE_RATIO").unwrap_or(0.8),
            decline_streak: parse_u64_env("BACKPRESSURE_INFLIGHT_DECLINE_STREAK").unwrap_or(3),
            reject_rate_beta: parse_f64_env("BACKPRESSURE_INFLIGHT_REJECT_BETA").unwrap_or(0.2),
        }
    }
}

pub struct InflightController {
    config: InflightControllerConfig,
    cmd_rx: mpsc::UnboundedReceiver<InflightCommand>,
    inflight: u64,
    limit_tx: watch::Sender<u64>,
    inflight_tx: watch::Sender<u64>,
    rate_ewma_milli_tx: watch::Sender<u64>,
    soft_reject_rate_milli_tx: watch::Sender<u64>,
    rate_declining_tx: watch::Sender<bool>,
    last_sample: Instant,
    last_committed_total: u64,
    committed_total: u64,
    soft_reject_total: u64,
    rate_ewma: f64,
    reject_rate_ewma: f64,
    last_soft_reject_total: u64,
    last_rate_ewma: f64,
    decline_streak: u64,
    rejecting: bool,
}

impl InflightController {
    pub fn spawn_from_env() -> InflightControllerHandle {
        let config = InflightControllerConfig::from_env();
        let initial_limit = config.initial_limit.max(1);
        let (limit_tx, limit_rx) = watch::channel(initial_limit);
        let (inflight_tx, inflight_rx) = watch::channel(0u64);
        let (rate_ewma_milli_tx, rate_ewma_milli_rx) = watch::channel(0u64);
        let (soft_reject_rate_milli_tx, soft_reject_rate_milli_rx) = watch::channel(0u64);
        let (rate_declining_tx, rate_declining_rx) = watch::channel(false);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let handle = InflightControllerHandle {
            enabled: config.enabled,
            cmd_tx,
            inflight_rx,
            limit_rx,
            rate_ewma_milli_rx,
            soft_reject_rate_milli_rx,
            rate_declining_rx,
        };

        let controller = Self::new(
            config,
            cmd_rx,
            limit_tx,
            inflight_tx,
            rate_ewma_milli_tx,
            soft_reject_rate_milli_tx,
            rate_declining_tx,
        );
        tokio::spawn(async move {
            controller.run().await;
        });

        handle
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        config: InflightControllerConfig,
        cmd_rx: mpsc::UnboundedReceiver<InflightCommand>,
        limit_tx: watch::Sender<u64>,
        inflight_tx: watch::Sender<u64>,
        rate_ewma_milli_tx: watch::Sender<u64>,
        soft_reject_rate_milli_tx: watch::Sender<u64>,
        rate_declining_tx: watch::Sender<bool>,
    ) -> Self {
        let target = config.target_wal_age_sec.max(0.001);
        let rate_ewma = (config.initial_limit as f64) / target;
        let _ = rate_ewma_milli_tx.send((rate_ewma * 1000.0) as u64);
        Self {
            config,
            cmd_rx,
            inflight: 0,
            limit_tx,
            inflight_tx,
            rate_ewma_milli_tx,
            soft_reject_rate_milli_tx,
            rate_declining_tx,
            last_sample: Instant::now(),
            last_committed_total: 0,
            committed_total: 0,
            soft_reject_total: 0,
            rate_ewma,
            reject_rate_ewma: 0.0,
            last_soft_reject_total: 0,
            last_rate_ewma: rate_ewma,
            decline_streak: 0,
            rejecting: false,
        }
    }

    async fn run(mut self) {
        let mut interval = tokio::time::interval(Duration::from_millis(self.config.tick_ms));
        loop {
            tokio::select! {
                maybe_cmd = self.cmd_rx.recv() => {
                    match maybe_cmd {
                        Some(cmd) => self.handle_cmd(cmd),
                        None => break,
                    }
                }
                _ = interval.tick() => {
                    self.tick();
                }
            }
        }
    }

    fn handle_cmd(&mut self, cmd: InflightCommand) {
        match cmd {
            InflightCommand::Reserve { resp } => {
                let decision = self.reserve();
                let _ = resp.send(decision);
            }
            InflightCommand::Release { n } => {
                self.inflight = self.inflight.saturating_sub(n);
                let _ = self.inflight_tx.send(self.inflight);
            }
            InflightCommand::Commit { n } => {
                self.inflight = self.inflight.saturating_sub(n);
                self.committed_total = self.committed_total.saturating_add(n);
                let _ = self.inflight_tx.send(self.inflight);
            }
        }
    }

    fn reserve(&mut self) -> ReserveDecision {
        let limit = *self.limit_tx.borrow();
        let inflight = self.inflight;
        let off_threshold = (limit as f64 * self.config.hysteresis_off_ratio) as u64;

        if !self.config.enabled {
            self.inflight = self.inflight.saturating_add(1);
            let _ = self.inflight_tx.send(self.inflight);
            return ReserveDecision::Allow {
                inflight: self.inflight,
                limit,
            };
        }

        if *self.rate_declining_tx.borrow() {
            self.soft_reject_total = self.soft_reject_total.saturating_add(1);
            return ReserveDecision::RejectRateDecline { inflight, limit };
        }

        if self.rejecting {
            if inflight >= off_threshold {
                self.soft_reject_total = self.soft_reject_total.saturating_add(1);
                return ReserveDecision::RejectInflight { inflight, limit };
            }
            self.rejecting = false;
        }

        if inflight >= limit {
            self.rejecting = true;
            self.soft_reject_total = self.soft_reject_total.saturating_add(1);
            return ReserveDecision::RejectInflight { inflight, limit };
        }

        self.inflight = self.inflight.saturating_add(1);
        let _ = self.inflight_tx.send(self.inflight);
        ReserveDecision::Allow {
            inflight: self.inflight,
            limit,
        }
    }

    fn tick(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_sample);
        if dt < Duration::from_millis(50) {
            return;
        }

        let dt_sec = dt.as_secs_f64().max(0.000_001);
        let committed_delta = self.committed_total.saturating_sub(self.last_committed_total);
        let instant_rate = (committed_delta as f64) / dt_sec;

        self.rate_ewma = (1.0 - self.config.beta) * self.rate_ewma
            + self.config.beta * instant_rate;
        let _ = self
            .rate_ewma_milli_tx
            .send((self.rate_ewma * 1000.0) as u64);

        if self.rate_ewma < self.last_rate_ewma * self.config.decline_ratio {
            self.decline_streak = self.decline_streak.saturating_add(1);
        } else {
            self.decline_streak = 0;
        }
        let _ = self
            .rate_declining_tx
            .send(self.decline_streak >= self.config.decline_streak);
        self.last_rate_ewma = self.rate_ewma;

        let soft_reject_delta = self
            .soft_reject_total
            .saturating_sub(self.last_soft_reject_total);
        let instant_reject_rate = (soft_reject_delta as f64) / dt_sec;
        self.reject_rate_ewma = (1.0 - self.config.reject_rate_beta) * self.reject_rate_ewma
            + self.config.reject_rate_beta * instant_reject_rate;
        let _ = self
            .soft_reject_rate_milli_tx
            .send((self.reject_rate_ewma * 1000.0) as u64);
        self.last_soft_reject_total = self.soft_reject_total;

        if self.config.enabled {
            let target = self.config.target_wal_age_sec.max(0.001);
            let raw = self.config.safety_alpha * self.rate_ewma * target;
            let mut desired = raw.round() as u64;
            desired = desired.clamp(self.config.inflight_min, self.config.inflight_cap);

            let current = *self.limit_tx.borrow();
            let max_step = ((current as f64) * self.config.max_step_ratio).max(1.0) as u64;
            let next = if desired > current + max_step {
                current + max_step
            } else if desired + max_step < current {
                current.saturating_sub(max_step)
            } else {
                desired
            };
            if next != current {
                let _ = self.limit_tx.send(next);
            }
        }

        self.last_committed_total = self.committed_total;
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
